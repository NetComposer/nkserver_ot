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

-module(nkserver_ot).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([span/2, span/3, new/3, new/4, finish/1, delete/1]).
-export([tag/3, tags/2, tag_error/2, log/2, log/3, make_parent/1, get_span/1, update/2]).
-export([get_id/1, update_name/2, update_srv_id/2, update_trace_id/2, update_parent/2]).
-export([trace_id_hex/1, trace_id_to_bin/1, bin_to_trace_id/1]).
-export([span_id_hex/1, span_id_to_bin/1, bin_to_span_id/1]).
-export_type([id/0, span/0, span_id/0, name/0, time/0, info/0, span_code/0, trace_code/0]).

-compile(inline).
%-compile({no_auto_import, [get/1, put/2]}).

-include("nkserver_ot.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type id() :: span() | span_id().
-type span() :: #span{} | undefined.
-type span_id() :: term().
-type time() :: nklib_date:epoch(usecs).
-type name() :: binary().
-type info() :: binary() | iolist() | atom() | integer().
-type key() :: atom() | list() | binary().
-type value() :: atom() | list() | binary() | integer() | boolean().
-type trace_code() :: term().
-type span_code() :: term().
-type parent() :: #span_parent{}.

%% ===================================================================
%% Public
%% ===================================================================

%% @doc Creates a new span without parent
-spec span(nkserver:id(), name()) ->
    span().

span(SrvId, Name) when is_atom(SrvId) ->
    span(SrvId, Name, undefined).


%% @doc Creates a new span with a parent
-spec span(nkserver:id(), name(), parent()|span_id()|span()|undefined) ->
    span().

span(SrvId, Name, undefined) when is_atom(SrvId) ->
    span(SrvId, Name, #span_parent{});

span(SrvId, Name, #span_parent{trace_code=TraceCode, span_code=ParentCode}) when
    is_atom(SrvId) andalso
        (is_integer(TraceCode) orelse TraceCode==undefined) orelse
        (is_integer(ParentCode) orelse ParentCode==undefined) ->
    TraceCode2 = case TraceCode of
        undefined -> make_id();
        _ -> TraceCode
    end,
    #span{
        srv = SrvId,
        app = to_bin(SrvId),
        timestamp = nklib_date:epoch(usecs),
        trace_code = TraceCode2,
        span_code = make_id(),
        parent_code = ParentCode,
        name = to_bin(Name)
    };

span(SrvId, Name, #span{}=ParentSpanId) ->
    span(SrvId, Name, make_parent(ParentSpanId));

span(SrvId, Name, SpanId) ->
    span(SrvId, Name, get_span(SpanId)).



%% @doc Creates a new span without parent, and stores it in process dictionary
-spec new(span_id(), nkserver:id(), name()) ->
    span_id().

new(SpanId, SrvId, Name) when is_atom(SrvId) ->
    put_span(SpanId, span(SrvId, Name)).


%% @doc Creates a new span with a parent, and stores it in process dictionary
-spec new(span_id(), nkserver:id(), name(), parent()|span_id()|span()|undefined) ->
    span_id().

new(SpanId, SrvId, Name, undefined) when is_atom(SrvId) ->
    new(SpanId, SrvId, Name, #span_parent{});

new(SpanId, SrvId, Name, #span_parent{}=Parent) when is_atom(SrvId) ->
    put_span(SpanId, span(SrvId, Name, Parent));

new(SpanId, SrvId, Name, #span{}=ParentSpan) ->
    new(SpanId, SrvId, Name, make_parent(ParentSpan));

new(SpainId, SrvId, Name, ParentSpanId) ->
    new(SpainId, SrvId, Name, get_span(ParentSpanId)).


%% @doc Adds a new tag to a span
-spec tag(id(), key(), value()) ->
    span().

tag(undefined, _Key, _Value) ->
    undefined;

tag(#span{}=Span, Key, Value) ->
    add_tag(Span, Key, Value);

tag(SpanId, Key, Value) ->
    put_span(SpanId, add_tag(get_span(SpanId), Key, Value)).


%% @doc
-spec tag_error(id(), nkserver:status()) ->
    span().

tag_error(undefined, _Error) ->
    undefined;

tag_error(#span{srv=SrvId}=Span, Error) ->
    #{status:=Status} = Map = nkserver_status:extended_status(SrvId, Error),
    tags(Span, #{
        <<"error">> => true,
        <<"error.code">> => Status,
        <<"error.reason">> => maps:get(info, Map, <<>>)
    });

tag_error(SpanId, Error) ->
    put_span(SpanId, tag_error(get_span(SpanId), Error)).


%% @doc Adds a set of tags to an in-process span
-spec tags(id(), map()) ->
    span().

tags(undefined, _Tags) ->
    undefined;

tags(#span{}=Span, Tags) ->
    lists:foldl(
        fun({Key, Val}, Acc) -> add_tag(Acc, Key, Val) end,
        Span,
        maps:to_list(Tags));

tags(SpanId, Tags) ->
    put_span(SpanId, tags(get_span(SpanId), Tags)).


%% @doc Adds a log to a span
-spec log(id(), key()|{list()|binary(), list()}) ->
    span().

log(undefined, _Text) ->
    undefined;

log(#span{}=Span, Log) ->
    add_log(Span, Log);

log(SpanId, Log) ->
    put_span(SpanId, log(get_span(SpanId), Log)).


%% @doc Adds a log to a span with formatting
-spec log(id(), list()|binary(), list()) ->
    span().

log(undefined, _Fmt, _List) ->
    undefined;

log(#span{}=Span, Fmt, List) when is_list(List) ->
    add_log(Span, {Fmt, List});

log(SpanId, Fmt, List) when is_list(List) ->
    put_span(SpanId, log(get_span(SpanId), Fmt, List)).



%% @doc Get TraceCode and ParentId for a span
-spec make_parent(id()|parent()|undefined) ->
    #span_parent{} | undefined.

make_parent(undefined) ->
    undefined;

make_parent(#span_parent{}=SpanParent) ->
    SpanParent;

make_parent(#span{trace_code=TraceCode, span_code=SpanCode}) ->
    #span_parent{trace_code=TraceCode, span_code=SpanCode};

make_parent(SpanId) ->
    make_parent(get_span(SpanId)).


%% @doc Finish a span
-spec finish(id()) ->
    id() | span().

finish(undefined) ->
    undefined;

finish(#span{logs=Logs, timestamp=Start}=Span) ->
    Span2 = Span#span{
        duration = nklib_date:epoch(usecs) - Start,
        logs = lists:reverse(Logs)
    },
    nkserver_ot_rules:span(Span2),
    Span2;

finish(SpanId) ->
    Span = get_span(SpanId),
    Span2 = finish(Span),
    put_span(SpanId, undefined),
    Span2.


%% @doc Deletes a span without sending
-spec delete(id()) ->
    ok.

delete(undefined) ->
    ok;

delete(#span{}) ->
    ok;

delete(SpanId) ->
    put_span(SpanId, undefined),
    ok.


%% @doc
get_id(undefined) ->
    ok;

get_id(#span{trace_code=Trace, span_code=Span}) ->
    {Trace, Span};

get_id(SpanId) ->
    get_id(get_span(SpanId)).


%% @doc
update(undefined, _Updates) ->
    ok;

update(#span{}=Span, Updates) ->
    do_updates(Updates, Span);

update(SpanId, Updates) ->
    case get_span(SpanId) of
        undefined ->
            ok;
        #span{}=Span ->
            Span2 = update(Span, Updates),
            put_span(SpanId, Span2),
            ok
    end.




%% ===================================================================
%% Internal
%% ===================================================================

%% @private
do_updates([], Span) ->
    Span;

do_updates([{srv, SrvId}|Rest], Span) when is_atom(SrvId) ->
    do_updates(Rest, Span#span{srv=SrvId});

do_updates([{app, App}|Rest], Span) ->
    do_updates(Rest, Span#span{app=to_bin(App)});

do_updates([{name, Name}|Rest], Span) ->
    do_updates(Rest, Span#span{name=to_bin(Name)});

do_updates([{trace_code, TraceCode}|Rest], Span) when is_integer(TraceCode) ->
    do_updates(Rest, Span#span{trace_code=TraceCode});

do_updates([{parent, Parent}|Rest], Span) ->
    do_updates(Rest, update_parent(Span, Parent)).



%% @private
update_name(undefined, _SrvId) ->
    undefined;

update_name(#span{}=Span, Name) ->
    Span#span{name=to_bin(Name)};

update_name(SpanId, Name) ->
    put_span(SpanId, update_name(get_span(SpanId), Name)).


%% @private
update_srv_id(undefined, _SrvId) ->
    undefined;

update_srv_id(#span{}=Span, SrvId) when is_atom(SrvId) ->
    Span#span{srv=SrvId};

update_srv_id(SpanId, SrvId) ->
    put_span(SpanId, update_srv_id(get_span(SpanId), SrvId)).


%% @private
update_trace_id(undefined, _SrvId) ->
    undefined;

update_trace_id(#span{}=Span, TraceCode) when is_integer(TraceCode) ->
    Span#span{trace_code = TraceCode};

update_trace_id(SpanId, SrvId) ->
    put_span(SpanId, update_trace_id(get_span(SpanId), SrvId)).


%% @private
update_parent(undefined, _Parent) ->
    undefined;

update_parent(#span{}=Span, #span_parent{trace_code=TraceCode, span_code=ParentCode}) ->
    Span#span{
        trace_code = TraceCode,
        parent_code = ParentCode
    };

update_parent(SpanId, Parent) ->
    put_span(SpanId, update_parent(get_span(SpanId), Parent)).


%% @private
make_id() ->
    bin_to_trace_id(crypto:strong_rand_bytes(8)).


%% @private
trace_id_hex(undefined) ->
    <<>>;

trace_id_hex(#span{trace_code=TraceCode}) ->
    nklib_util:hex(trace_id_to_bin(TraceCode));

trace_id_hex(SpanId) ->
    trace_id_hex(get_span(SpanId)).


%% @private
trace_id_to_bin(TraceCode) ->
    <<TraceCode:64/signed-integer>>.


%% @private
bin_to_trace_id(Bin) ->
    <<TraceCode:64/signed-integer>> = Bin,
    TraceCode.


%% @private
span_id_hex(undefined) ->
    <<>>;

span_id_hex(#span{span_code=SpanCode}) ->
    nklib_util:hex(span_id_to_bin(SpanCode));

span_id_hex(SpanId) ->
    span_id_hex(get_span(SpanId)).


%% @private
span_id_to_bin(SpanCode) ->
    <<SpanCode:64/signed-integer>>.


%% @private
bin_to_span_id(Bin) ->
    <<SpanCode:64/signed-integer>> = Bin,
    SpanCode.



%% @private
add_tag(undefined, _Key, _Value) ->
    undefined;

add_tag(#span{tags=Tags}=Span, Key, Value) ->
    Tags2 = Tags#{to_bin(Key) => {Value, undefined}},
    Span#span{tags = Tags2}.


%% @private
%% Format will be expanded in nkserver_ot_encode, to allow {Fmt, Args}, iolist, etc.
add_log(undefined, _Val) ->
    undefined;

add_log(#span{logs=Logs}=Span, Val) ->
    Logs2 = [{nklib_date:epoch(usecs), Val, undefined} | Logs],
    Span#span{logs = Logs2}.


%% @private Get top span in process dictionary
-spec get_span(span_id()) ->
    span() | undefined.

get_span(SpanId) ->
    case get({nkserver_ot_span, SpanId}) of
        #span{}=Span ->
            Span;
        undefined ->
            undefined
    end.


%% @private
put_span(SpanId, Span) ->
    put({nkserver_ot_span, SpanId}, Span),
    SpanId.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).

