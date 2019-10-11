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


-module(nkserver_ot_sender).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([store_span/1, pause/1, get_total/0]).
-export([start_link/0]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).
-export([split/2]).

-include("nkserver_ot.hrl").


%% ===================================================================
%% Public
%% ===================================================================

%% @doc
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% @doc
store_span(Span) ->
    gen_server:cast(?MODULE, {new_span, Span}).


%% @doc
get_total() ->
    gen_server:call(?MODULE, get_total).


%% @doc
pause(Boolean) ->
    nklib_util:do_config_put(nkserver_ot_pause_sender, Boolean).


%% ===================================================================
%% gen_server
%% ===================================================================

-record(state, {
    url :: string() | undefined,
    interval :: integer() | undefined,
    spans = [] :: [#span{}],
    total = 0 :: integer()
}).


%% @private
init([]) ->
    pause(false),
    case nkserver_ot_app:get(activate) of
        false ->
            Url = nkserver_ot_app:get(opentrace_url),
            ok = hackney_pool:start_pool(?MODULE, []),
            ok = hackney_pool:set_max_connections(?MODULE, 2),
            ok = hackney_pool:set_timeout(?MODULE, 10000),
            10000 = hackney_pool:timeout(?MODULE),
            Time = nkserver_ot_app:get(opentrace_interval),
            State = #state{url = binary_to_list(Url), interval = Time},
            lager:notice("Starting NkSERVER SPAN Sender (url:~s, interval:~p)", [Url, Time]),
            self() ! send_spans,
            {ok, State};
        false ->
            {ok, #state{}}
    end.


%% @private
handle_call(get_total, _From, #state{total=Total}=State) ->
    {reply, {ok, Total}, State};

handle_call(Msg, _From, State) ->
    lager:error("Received unexpected call at ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_cast({new_span, _Span}, #state{url=undefined}=State) ->
    {noreply, State};

handle_cast({new_span, Span}, #state{spans=Spans, total=Total}=State) ->
    {noreply, State#state{spans=[Span|Spans], total=Total+1}};

handle_cast(Msg, State) ->
    lager:error("Received unexpected call at ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_info(send_spans, #state{spans=[], interval=Time}=State) ->
    erlang:send_after(Time, self(), send_spans),
    {noreply, State};

handle_info(send_spans, #state{interval=Time, total=Total}=State) ->
    State2 = case nklib_util:do_config_get(nkserver_ot_pause_sender, false) of
        true ->
            {message_queue_len, Len} = process_info(self(), message_queue_len),
            lager:error("NKLOG SKIPING SENDING SPANS ~p (~p)", [Len, Total]),
            State;
        _ ->
            do_send_spans(State)
    end,
    erlang:send_after(Time, self(), send_spans),
    {noreply, State2#state{spans=[]}};

handle_info(Msg, State) ->
    lager:error("Received unexpected info at ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
terminate(_Reason, _State) ->
    ok.



%% ===================================================================
%% Internal
%% ===================================================================


%% @private
do_send_spans(#state{url=Url, spans=Spans, total=Total}=State) ->
    case Total > 1000 of
        true ->
            {Spans2, Rest} = split(Spans, 1000),
            do_send_spans(Url, Spans2, Total),
            do_send_spans(State#state{spans=Rest, total=Total-1000});
        false ->
            do_send_spans(Url, Spans, Total),
            State#state{spans=[], total=0}
    end.


%% @private
do_send_spans(Url, Spans, Total) ->
    Data2 = nkserver_ot_encode:encode(Spans),
    Hds = [{<<"Content-Type">>, <<"application/x-thrift">>}],
    Opts = [
        {pool, ?MODULE},
        with_body
    ],
    case hackney:request(post, Url, Hds, Data2, Opts) of
        {ok, 202, _, _} ->
            {message_queue_len, QueueLen} = process_info(self(), message_queue_len),
            case Total >= 1000 of
                true ->
                    lager:info("Sent ~p/~p SPANs (waiting ~p)", [1000, Total, QueueLen]);
                false ->
                    lager:info("Sent ~p SPANs (waiting ~p)", [Total, QueueLen])
            end,
            nkserver_ot_snapshots:snapshot(
                [?MODULE, send_buffer, ok],
                [{spans, length(Spans)}]);
        Error ->
            lager:warning("Error sending spans: ~p", [Error]),
            nkserver_ot_snapshots:snapshot(
                [?MODULE, send_buffer, failed],
                [{spans, length(Spans)}, {error, Error}])
    end.


%% @private
split(List, Max) ->
    split(List, 0, Max, []).


%% @private
split([], _Pos, _Max, Acc) ->
    {Acc, []};

split([Next|Rest], Pos, Max, Acc) when Pos < Max ->
    split(Rest, Pos+1, Max, [Next|Acc]);

split([Next|Rest], _Pos, _Max, Acc) ->
    {Acc, [Next|Rest]}.
