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


-module(nkserver_ot_snapshots).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([snapshot/2, list_counts/0, get_snap/1, delete_counter/1, delete_all_counters/0]).
-export([start_link/0]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
    handle_info/2]).


%% ===================================================================
%% Types
%% ===================================================================


%% ===================================================================
%% Public
%% ===================================================================

%% @doc
snapshot(Key, [{_, _} |_ ] = Data) ->
    {_, _, Us} = os:timestamp(),
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:local_time(),
    ets:insert(
        nkserver_ot_snapshots_store,
        {
            Key,
            [
                {snap_timestamp, {Year, Month, Day, Hour, Min, Sec, Us}}
                | Data
            ]
        }
    ),
    case catch ets:update_counter(nkserver_ot_snapshots_count, Key, 1) of
        {'EXIT', {badarg, _}} ->
            ets:insert(nkserver_ot_snapshots_count, {Key, 1});
        Cnt ->
            Cnt
    end;

snapshot(Key, Data) ->
    snapshot(Key, [{data, Data}]).


%% @doc
list_counts() ->
    ets:tab2list(nkserver_ot_snapshots_count).


%% @doc
get_snap(Key) ->
    ets:lookup(nkserver_ot_snapshots_store, Key).


%% @doc
delete_counter(Key) ->
    ets:delete(nkserver_ot_snapshots_store, Key),
    ets:delete(nkserver_ot_snapshots_count, Key),
    ok.


%% @doc
delete_all_counters() ->
    ets:delete_all_objects(nkserver_ot_snapshots_store),
    ets:delete_all_objects(nkserver_ot_snapshots_count),
    ok.


%% ===================================================================
%% gen_server
%% ===================================================================

%% @doc
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).



-record(state, {
}).


%% @private
init([]) ->
    [
        ets:new(Tab, [named_table, public, {Concurrency, true}])
        || {Tab, Concurrency} <- [
            {nkserver_ot_snapshots_count, write_concurrency},
            {nkserver_ot_snapshots_store, write_concurrency}
        ]
    ],
    {ok, #state{}}.


%% @private
handle_call(Msg, _From, State) ->
    lager:error("Received unexpected call at ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_cast(Msg, State) ->
    lager:error("Received unexpected call at ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_info(Msg, State) ->
    lager:error("Received unexpected info at ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
terminate(_Reason, _State) ->
    ok.



