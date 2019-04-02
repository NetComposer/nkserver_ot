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
-export([span/2, span/3, new/3, new/4, finish/1]).
-export([tag/3, tags/2, tag_error/2, log/2, get_parent/1, get_span/1]).
-export([update_srv_id/2, update_trace_id/2, update_parent/2]).
-export([trace_id_hex/1, trace_id_to_bin/1, bin_to_trace_id/1]).
-export_type([span/0, id/0, name/0, time/0, info/0, trace_id/0, span_id/0]).

-compile(inline).
%-compile({no_auto_import, [get/1, put/2]}).

-include("nkserver_ot.hrl").
-include("nkserver_ot.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type span() :: #span{} | undefined.
-type id() :: term().


-type time() :: nklib_date:epoch(usecs).

-type name() :: binary().
-type info() :: binary() | iolist() | atom() | integer().
-type key() :: atom() | list() | binary().
-type value() :: atom() | list() | binary() | integer() | boolean().
-type trace_id() :: binary().
-type span_id() :: binary().
-type parent() :: {trace_id(), span_id()}|undefined.

%% ===================================================================
%% Public
%% ===================================================================

%% @doc Creates a new span without parent
-spec span(nkserver:id(), name()) ->
    span().

span(SrvId, Name) when is_atom(SrvId) ->
    span(SrvId, Name, undefined).


%% @doc Creates a new span with a parent
-spec span(nkserver:id(), name(), parent()|id()|span()|undefined) ->
    span().

span(SrvId, Name, undefined) when is_atom(SrvId) ->
    span(SrvId, Name, {undefined, undefined});

span(SrvId, Name, {TraceId, ParentId}) when
    is_atom(SrvId) andalso
        (is_integer(TraceId) orelse TraceId==undefined) orelse
        (is_integer(ParentId) orelse ParentId==undefined) ->
    TraceId2 = case TraceId of
        undefined -> make_id();
        _ -> TraceId
    end,
    #span{
        srv = SrvId,
        timestamp = nklib_date:epoch(usecs),
        trace_id = TraceId2,
        id = make_id(),
        parent_id = ParentId,
        name = Name
    };

span(SrvId, Name, #span{}=ParentSpan) ->
    span(SrvId, Name, get_parent(ParentSpan));

span(SrvId, Name, ParentId) ->
    span(SrvId, Name, get_span(ParentId)).



%% @doc Creates a new span without parent, and stores it in process dictionary
-spec new(id(), nkserver:id(), name()) ->
    span().

new(Id, SrvId, Name) when is_atom(SrvId) ->
    put_span(Id, span(SrvId, Name)).


%% @doc Creates a new span with a parent, and stores it in process dictionary
-spec new(id(), nkserver:id(), name(), parent()|id()|span()|undefined) ->
    span().

new(Id, SrvId, Name, undefined) when is_atom(SrvId) ->
    new(Id, SrvId, Name, {undefined, undefined});

new(Id, SrvId, Name, {TraceId, ParentId}) when is_atom(SrvId) ->
    put_span(Id, span(SrvId, Name, {TraceId, ParentId}));

new(Id, SrvId, Name, #span{}=ParentSpan) ->
    new(Id, SrvId, Name, get_parent(ParentSpan));

new(Id, SrvId, Name, ParentId) ->
    new(Id, SrvId, Name, get_span(ParentId)).


%% @doc Adds a new tag to a span
-spec tag(id()|span(), key(), value()) ->
    span().

tag(undefined, _Key, _Value) ->
    undefined;

tag(#span{}=Span, Key, Value) ->
    add_tag(Span, Key, Value);

tag(Id, Key, Value) ->
    put_span(Id, add_tag(get_span(Id), Key, Value)).


%% @doc
-spec tag_error(id()|span(), nkserver:msg()) ->
    span().

tag_error(undefined, _Error) ->
    undefined;

tag_error(#span{srv=SrvId}=Span, Error) ->
    {Code, Reason} = nkserver_msg:msg(SrvId, Error),
    tags(Span, #{
        <<"error">> => true,
        <<"error.code">> => Code,
        <<"error.reason">> => Reason
    });

tag_error(Id, Error) ->
    put_span(Id, tag_error(get_span(Id), Error)).


%% @doc Adds a set of tags to an in-process span
-spec tags(id()|span(), map()) ->
    span().

tags(undefined, _Tags) ->
    undefined;

tags(#span{}=Span, Tags) ->
    lists:foldl(
        fun({Key, Val}, Acc) -> add_tag(Acc, Key, Val) end,
        Span,
        maps:to_list(Tags));

tags(Id, Tags) ->
    put_span(Id, tags(get_span(Id), Tags)).


%% @doc Adds a log to a span
-spec log(id()|span(), key()|{list(), list()}) ->
    span().

log(undefined, _Text) ->
    undefined;

log(#span{}=Span, Log) ->
    add_log(Span, Log);

log(Id, Log) ->
    put_span(Id, log(get_span(Id), Log)).


%% @doc Get TraceId and ParentId for a span
-spec get_parent(id()|span()|undefined) ->
    {trace_id()|undefined, span_id()|undefined}.

get_parent(undefined) ->
    {undefined, undefined};

get_parent(#span{trace_id=TraceId, id=Id}) ->
    {TraceId, Id};

get_parent(Id) ->
    get_parent(get_span(Id)).


%% @doc Finish a span
-spec finish(id()|span()) ->
    ok.

finish(undefined) ->
    undefined;

finish(#span{logs=Logs, timestamp=Start}=Span) ->
    Span2 = Span#span{
        duration = nklib_date:epoch(usecs) - Start,
        logs = lists:reverse(Logs)
    },
    nkserver_ot_rules:span(Span2),
    Span2;

finish(Id) ->
    Span = get_span(Id),
    Span2 = finish(Span),
    put_span(Id, undefined),
    Span2.



%% ===================================================================
%% Internal
%% ===================================================================

%% @private
update_srv_id(undefined, _SrvId) ->
    undefined;

update_srv_id(#span{}=Span, SrvId) when is_atom(SrvId) ->
    Span#span{srv=SrvId};

update_srv_id(Id, SrvId) ->
    put_span(Id, update_srv_id(get_span(Id), SrvId)).


%% @private
update_trace_id(undefined, _SrvId) ->
    undefined;

update_trace_id(#span{}=Span, TraceId) when is_integer(TraceId) ->
    Span#span{trace_id = TraceId};

update_trace_id(Id, SrvId) ->
    put_span(Id, update_trace_id(get_span(Id), SrvId)).


%% @private
update_parent(undefined, _SrvId) ->
    undefined;

update_parent(#span{}=Span, {TraceId, ParentId})
    when is_integer(TraceId), is_integer(ParentId) ->
    Span#span{
        trace_id = TraceId,
        parent_id = ParentId
    };

update_parent(Id, SrvId) ->
    put_span(Id, update_parent(get_span(Id), SrvId)).


%% @private
make_id() ->
    bin_to_trace_id(crypto:strong_rand_bytes(8)).


%% @private
trace_id_hex(#span{trace_id=TraceId}) ->
    nklib_util:hex(trace_id_to_bin(TraceId)).


%% @private
trace_id_to_bin(TraceId) ->
    <<TraceId:64/signed-integer>>.


%% @private
bin_to_trace_id(Bin) ->
    <<TraceId:64/signed-integer>> = Bin,
    TraceId.


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
-spec get_span(id()) ->
    span() | undefined.

get_span(Id) ->
    case get({nkserver_ot_span, Id}) of
        #span{}=Span ->
            Span;
        undefined ->
            undefined
    end.


%% @private
put_span(Id, Span) ->
    put({nkserver_ot_span, Id}, Span),
    Span.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).


