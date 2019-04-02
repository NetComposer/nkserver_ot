%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.
%%
%% Based partially on 'otters' project by Heinz N. Gies
%%
%% Copyright (c) 2017 Heinz N. Gies
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to
%% deal in the Software without restriction, including without limitation the
%% rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
%% sell copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.</br>
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING,
%% FROM OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
%% IN THE SOFTWARE.
%%
%% -------------------------------------------------------------------
%%
%% https://github.com/project-fifo/otters
%%
%% Format for filters:
%% <name>([condition]) -> <action>.
%%
%% Conditions:
%% - none (my_rule() -> send.): always matches
%% - <key> >|<|>=|=<|==|/= <value>
%%
%% Actions:
%% - drop: stops filtering for this and the following rules.
%% - skip: skips the rest of this rule and continues with the next.
%% - send: sends the span (and continues)
%% - continue: if condition is true continue, otherwise stop
%% - count(<values>, ...)
%%      where values can either be a string 'bla' which is taken literally or
%%      a keyword span_duration to lookup the value.
%%
%% Sample:
%%
%% slow_spans(span_duration > 5000000) -> continue.
%% slow_spans(span_name == 'radius request') -> continue.
%% slow_spans() -> count('long_radius_request').
%% slow_spans() -> send.
%%
%% count() -> count('request', span_name, final_result).
%%
%%
%%
%%
%%
%%
%%
%%
%%
%%

-module(nkserver_ot_rules).
-include("nkserver_ot.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-export([get_rules_mod/1, span/1, compile/2, clear/1]).

-define(DURATION, "span_duration").
-define(NAME, "span_name").


%% @doc 
get_rules_mod(SrvId) ->
    list_to_atom(atom_to_list(SrvId) ++ "_nkserver_ot_rules").


%% @doc Compiles a new module
compile(SrvId, S) ->
    S2 = nklib_util:to_list(S),
    {ok, T, _} = nkserver_ot_of_lexer:string(S2),
    {ok, Rs} = nkserver_ot_of_parser:parse(T),
    Rs1 = optimize(Rs),
    {ok, Cs} = group_rules(Rs1),
    Module = get_rules_mod(SrvId),
    Rendered = render(atom_to_list(Module), Cs),
    % io:format("MODULE ~p\n~s~n", [Module, Rendered]),
    {module, Module} = dynamic_compile:load_from_string(lists:flatten(Rendered)),
    ok.

%% @doc
clear(SrvId) ->
    compile(SrvId, "drop() -> drop.").


%% @doc
span(Span) ->
    {ok, Actions} = run(Span),
    %lager:error("NKLOG ACTIONS ~p", [Actions]),
    perform(lists:usort(Actions), Span).


%%%===================================================================
%%% Internal functions
%%%===================================================================

optimize([{_,undefined,continue} | R]) ->
    optimize(R);
optimize([]) ->
    [];
optimize([Rule | Rest]) ->
    [Rule | optimize(Rest)].

%%--------------------------------------------------------------------
%% @doc
%% Tests a span and performs the requested actions on it.
%% @end
%%--------------------------------------------------------------------
run(#span{srv=SrvId, tags=Tags, name=Name, duration=Duration}) ->
    case nkserver:get_cached_config(SrvId, nkserver_ot, rules_mod) of
        undefined ->
            {ok, []};
        RulesMod ->
            RulesMod:check(Tags, Name, Duration)
    end.


%% Since dialyzer will arn that the 'dummy'/empty implementation
%% of ol_filter can't ever match send or cout we have to ignore
%% this function
-dialyzer({nowarn_function, perform/2}).
perform([], _Span) ->
    ok;
perform([send | Rest], Span) ->
    nkserver_ot_sender:store_span(Span),
    perform(Rest, Span);
perform([{count, Path} | Rest], Span) ->
    nkserver_ot_snapshots:snapshot(Path, Span),
    perform(Rest, Span).

%%%===================================================================
%%% Compiler functions
%%%===================================================================
group_rules([]) ->
    {ok, []};

group_rules([{Name, Test, Result} | Rest]) ->
    group_rules(Rest, Name, [{Test, Result}], []).

group_rules([{Name, Test, Result} | Rest], Name, Conditions, Acc) ->
    group_rules(Rest, Name, [{Test, Result} | Conditions], Acc);
group_rules([{Name, Test, Result} | Rest], LastName, Conditions, Acc) ->
    case lists:keyfind(Name, 1, Acc) of
        false ->
            Acc1 = [{LastName, optimize_conditions(Conditions)} | Acc],
            group_rules(Rest, Name, [{Test, Result}], Acc1);
        _ ->
            {error, {already_defined, Name}}
    end;
group_rules([], LastName, Conditions, Acc) ->
    Acc1 = [{LastName, optimize_conditions(Conditions)} | Acc],
    {ok, optimize_rules(Acc1, [])}.

%% Continue on the end of a condition makes no sense, there is nothing to
%% continue to
optimize_conditions([{_, continue} | R]) ->
    optimize_conditions(R);
%% skip on the end of a condition makes no sense, there is nothing to skip
optimize_conditions([{_, skip} | R]) ->
    optimize_conditions(R);
optimize_conditions(R) ->
    lists:reverse(R).

optimize_rules([{_, []} | R], Acc) ->
    optimize_rules(R, Acc);
optimize_rules([E | R], Acc) ->
    optimize_rules(R, [E | Acc]);
optimize_rules([], Acc) ->
    Acc.

render(Module, []) ->
    ["-module("++Module++").\n",
     "-export([check/3]).\n",
     "-compile(inline).\n",
     "\n",
     "check(_Tags, _Name, _Duration) ->\n",
     "  {ok, []}.\n",
     "\n"];
render(Module, [{Name,_} | _] = Cs) ->
    ["-module("++Module++").\n",
     "-export([check/3]).\n",
     "-compile(inline).\n",
     "\n",
     "check(Tags, Name, Duration) ->\n",
     "  ", rule_name(Name, 0), "(Tags, Name, Duration, []).\n",
     "\n",
     "get_tag(Key, Tags) ->\n",
     "  KeyBin = nklib_util:to_binary(Key),\n",
     "  case maps:find(KeyBin, Tags) of\n",
     "    {ok, {V, _}} -> V;\n",
     "    _ -> <<>>\n",
     "  end.\n",
     "\n",
     render_(Cs)].

render_([{Name, Clauses} | [{NextName, _} | _] = R]) ->
    [render_clauses(Name, Clauses, NextName, 0), "\n",
     render_(R)];
render_([{Name, Clauses}]) ->
    [render_clauses(Name, Clauses, undefined, 0), "\n",
     "finish(_Tags, _Name, _Duration, Acc) ->\n",
     "  {ok, Acc}.\n"].

rule_name(undefined, _N) ->
    "finish";
rule_name(Name, N) ->
    ["rule_", Name, "_", integer_to_list(N)].

render_clauses(_Name, [], _NextRule, _N) ->
    "";

render_clauses(Name, [{undefined, drop} | R], NextRule, N) ->
    [rule_name(Name, N), "(_Tags, _Name, _Duration, Acc) ->\n",
     "  {ok, Acc}.\n\n",
     render_clauses(Name, R, NextRule, N + 1)];

%% Special case if we match for duration and name at once
render_clauses(Name, [{{_Cmp, ?DURATION, _V}, continue} = A,
                      {{_Cmp1, ?NAME, _V1}, continue} = B| R],
               NextRule, N) ->
    render_clauses(Name, [B, A| R], NextRule, N);

render_clauses(Name, [{{Cmp, ?NAME, V}, continue},
                      {{Cmp1, ?DURATION, V1}, continue}| R],
               NextRule, N) ->
    {Body, R1} = continue_body(R, Name, N, NextRule),
    [rule_name(Name, N),
     io_lib:format("(Tags, Name, Duration, Acc) when Name ~s ~s,"
                   " Duration ~s ~s->\n",
                   [Cmp, format_v(V), Cmp1, format_v(V1)]),
     "  ", Body, ";\n",
     rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "   ", rule_name(NextRule, 0), "(Tags, Name, Duration, Acc).\n\n",
     render_clauses(Name, R1, NextRule, N + 1)];


%% Speical cases for Duration
render_clauses(Name, [{{exists, ?DURATION}, Action} | R], NextRule, N) ->
    [rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "  ", render_action(Action, Name, N, NextRule, R), ".\n\n",
     render_clauses(Name, R, NextRule, N + 1)];

render_clauses(Name, [{{Cmp, ?DURATION, V}, continue} | R],
               NextRule, N) ->
    {Body, R1} = continue_body(R, Name, N, NextRule),
    [rule_name(Name, N),
     io_lib:format("(Tags, Name, Duration, Acc) when Duration ~s ~s ->\n",
                   [Cmp, format_v(V)]),
     "  ", Body, ";\n",
     rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "   ", rule_name(NextRule, 0), "(Tags, Name, Duration, Acc).\n\n",
     render_clauses(Name, R1, NextRule, N + 1)];

render_clauses(Name, [{{Cmp, ?DURATION, V}, Action} | R], NextRule, N) ->
    [rule_name(Name, N),
     io_lib:format("(Tags, Name, Duration, Acc) "
                   "when Duration ~s ~s ->\n",
                   [Cmp, format_v(V)]),
     "  ", render_action(Action, Name, N, NextRule, R), ";\n",
     rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "  ", next_rule(Name, N, NextRule, R),
     render_clauses(Name, R, NextRule, N + 1)];


%% Speical cases for Name
render_clauses(Name, [{{exists, ?NAME}, Action} | R], NextRule, N) ->
    [rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "  ", render_action(Action, Name, N, NextRule, R), ".\n\n",
     render_clauses(Name, R, NextRule, N + 1)];

render_clauses(Name, [{{Cmp, ?NAME, V}, continue} | R],
               NextRule, N) ->
    {Body, R1} = continue_body(R, Name, N, NextRule),
    [rule_name(Name, N),
     io_lib:format("(Tags, Name, Duration, Acc) when Name ~s ~s ->\n",
                   [Cmp, format_v(V)]),
     "  ", Body, ";\n",
     rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "   ", rule_name(NextRule, 0), "(Tags, Name, Duration, Acc).\n\n",
     render_clauses(Name, R1, NextRule, N + 1)];

render_clauses(Name, [{{Cmp, ?NAME, V}, Action} | R], NextRule, N) ->
    [rule_name(Name, N),
     io_lib:format("(Tags, Name, Duration, Acc) "
                   "when Name ~s ~s ->\n",
                   [Cmp, format_v(V)]),
     "  ", render_action(Action, Name, N, NextRule, R), ";\n",
     rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "  ", next_rule(Name, N, NextRule, R),
     render_clauses(Name, R, NextRule, N + 1)];

%% Normal cases
render_clauses(Name, [{{exists, Key}, Action} | R], NextRule, N) ->
    [rule_name(Name, N),
     io_lib:format("(Tags = #{<<\"~s\">> := _}, Name, Duration, Acc) ->\n",
                   [Key]),
     "  ", render_action(Action, Name, N, NextRule, R), ";\n",
     rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "  ", next_rule(Name, N, NextRule, R),
     render_clauses(Name, R, NextRule, N + 1)];


%% If we have mutliple continues in a row we can combine them,
%% this combines two continues.

render_clauses(Name, [{{Cmp1, Key1, V1}, continue},
                      {{Cmp2, Key2, V2}, continue}| R], NextRule, N) ->
    {Body, R1} = continue_body(R, Name, N, NextRule),
    [rule_name(Name, N),
     io_lib:format("(Tags = #{<<\"~s\">> := {_V1, _},"
                   " <<\"~s\">> := {_V2, _}}, Name, Duration, Acc) "
                   "when _V1 ~s ~s, "
                   " _V2 ~s ~s ->\n",
                   [Key1, Key2, Cmp1, format_v(V1), Cmp2, format_v(V2)]),
     "  ", Body, ";\n",
     rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "   ", next_rule(Name, N, NextRule, R1),
     render_clauses(Name, R1, NextRule, N + 1)];

render_clauses(Name, [{{Cmp, Key, V}, continue} | R], NextRule, N) ->
    {Body, R1} = continue_body(R, Name, N, NextRule),
    [rule_name(Name, N),
     io_lib:format("(Tags = #{<<\"~s\">> := {_V, _}}, Name, Duration, Acc) "
                   "when _V ~s ~s ->\n",
                   [Key, Cmp, format_v(V)]),
     "  ", Body, ";\n",
     rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "   ", next_rule(Name, N, NextRule, R1),
     render_clauses(Name, R1, NextRule, N + 1)];

render_clauses(Name, [{{Cmp, Key, V}, Action} | R], NextRule, N) ->
    [rule_name(Name, N),
     io_lib:format("(Tags = #{<<\"~s\">> := {_V, _}}, Name, Duration, Acc) "
                   "when _V ~s ~s ->\n",
                   [Key, Cmp, format_v(V)]),
     "  ", render_action(Action, Name, N, NextRule, R), ";\n",
     rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n",
     "  ", next_rule(Name, N, NextRule, R),
     render_clauses(Name, R, NextRule, N + 1)];

render_clauses(Name, [{undefined, Action} | R], NextRule, N) ->
    [rule_name(Name, N), "(Tags, Name, Duration, Acc) ->\n"
     "  ", render_action(Action, Name, N, NextRule, R), ".\n\n",
     render_clauses(Name, R, NextRule, N + 1)].

render_action(drop, _Name, _N, _NextRule, _R) ->
    "{ok, Acc}";
render_action(skip, _Name, _N, NextRule, _R) ->
    [rule_name(NextRule, 0), "(Tags, Name, Duration, Acc)"];

render_action(send, Name, N, NextRule, R) ->
    [next_rule_name(Name, N, NextRule, R), "(Tags, Name, Duration, [send | Acc])"];

render_action({count, Path}, Name, N, NextRule, R) ->
    [next_rule_name(Name, N, NextRule, R),
     "(Tags, Name, Duration, [{count, [", make_path(Path), "]} | Acc])"].


next_rule(Name, N, NextRule, R) ->
    [next_rule_name(Name, N, NextRule, R), "(Tags, Name, Duration, Acc).\n\n"].

next_rule_name(_Name, _N, NextRule, []) ->
    rule_name(NextRule, 0);
next_rule_name(Name, N, _NextRule, _R) ->
    rule_name(Name, N + 1).

make_path([E]) ->
    make_e(E);
make_path([E | R]) ->
    [make_e(E), ", ", make_path(R)].


make_e({get, ?NAME}) ->
    "Name";
make_e({get, ?DURATION}) ->
    "Duration";
make_e({get, V}) ->
    ["get_tag(", format_v(V), ", Tags)"];
make_e(V) ->
    format_v(V).

format_v(V) when is_integer(V) ->
    integer_to_list(V);
format_v(V) ->
    ["<<\"", V, "\">>"].


%% If a continue is followed by a always matching clause
%% We also pull the clause in
continue_body([{undefined, Action}| R], Name, N, NextRule) ->
    {render_action(Action, Name, N, NextRule, R), R};
continue_body(R, Name, N, _NextRule) ->
    {[rule_name(Name, N + 1), "(Tags, Name, Duration, Acc)"], R}.
