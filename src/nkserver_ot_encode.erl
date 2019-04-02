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

-module(nkserver_ot_encode).
-export([encode/1]).

-include("nkserver_ot.hrl").

-define(T_I16,     6).
-define(T_I32,     8).
-define(T_I64,    10).
-define(T_STRING, 11).
-define(T_STRUCT, 12).
-define(T_LIST,   15).


-record(conf,
{
    service                 :: binary(),
    ip                      :: non_neg_integer(),
    port                    :: non_neg_integer(),
    add_default_to_tag      :: boolean(),
    add_default_to_log      :: boolean(),
    server_bin_log_default  :: binary(),
    server_bin_tag_default  :: binary(),
    server_bin_log_undef    :: binary(),
    server_bin_tag_undef    :: binary()
}).


%% @doc
encode(#span{} = S) ->
    encode([S]);

encode(Spans) when is_list(Spans) ->
    Size = length(Spans),
    Defaults = #conf{
        ip = ip_to_i32({127, 0, 0, 1}),
        port = 0,
        add_default_to_tag = false,
        add_default_to_log = false
    },
    <<?T_STRUCT, Size:32,
        << << (encode_span(S, Defaults))/binary >>
            || S <- Spans
        >>/binary
    >>.


%% @doc
encode_span(Span, Defaults) ->
    #span{
        srv = SrvId,
        id = Id,
        trace_id = TraceId,
        name = Name,
        parent_id = ParentId,
        logs = Logs,
        tags = Tags,
        timestamp = Timestamp,
        duration = Duration
    } = Span,
    Cfg = config(SrvId, Defaults),
    % Needed to store the service for OpenZipkin
    Tags0Bin = encode_tag({<<"lc">>, {<<>>, default}}, Cfg),
    TagSize = maps:size(Tags)+1,
    LogSize = length(Logs),
    NameBin = to_bin(Name),
    ParentBin = case ParentId of
        undefined ->
            <<>>;
        ParentId ->
            <<?T_I64, 5:16, ParentId:64/signed-integer>>
    end,
    <<
        %% Header
        ?T_I64,    1:16, TraceId:64/signed-integer,
        ?T_STRING, 3:16, (byte_size(NameBin)):32, NameBin/binary,
        ?T_I64,    4:16, Id:64/signed-integer,
        ParentBin/binary,
        %% Logs
        ?T_LIST,   6:16, ?T_STRUCT, LogSize:32,
        (<< <<(encode_log(Log, Cfg))/binary>> || Log <- Logs >>)/bytes,
        %% Tags
        ?T_LIST,   8:16, ?T_STRUCT, TagSize:32, Tags0Bin/binary,
        (<< <<(encode_tag(Tag, Cfg))/binary>>
            || Tag <- maps:to_list(Tags)>>)/bytes,
        %% Tail
        ?T_I64,   10:16, Timestamp:64/signed-integer,
        ?T_I64,   11:16, Duration:64/signed-integer,
        0
    >>.



%% @doc
config(SrvId, Defaults) ->
    #conf{
        add_default_to_tag = DefToTag,
        add_default_to_log = DefToLog
    } = Defaults,
    Conf = Defaults#conf{service = to_bin(SrvId)},
    HostBin = encode_host(default, Conf),
    LogHostBin = <<?T_STRUCT, 3:16, HostBin/binary>>,
    TagHostBin = <<?T_STRUCT, 4:16, HostBin/binary>>,
    Conf#conf{
        server_bin_log_default = LogHostBin,
        server_bin_tag_default = TagHostBin,
        server_bin_log_undef = case DefToLog of
            true ->
                LogHostBin;
            _ ->
                <<>>
        end,
        server_bin_tag_undef = case DefToTag of
            true ->
                TagHostBin;
            _ ->
                <<>>
        end
    }.


%% @private
encode_host(default, Cfg) ->
    encode_host(Cfg#conf.service, Cfg);

encode_host(Service, Cfg) when is_binary(Service) ->
    encode_host({Service, Cfg#conf.ip, Cfg#conf.port}, Cfg);

encode_host({Service, Ip, Port}, _Cfg) when is_integer(Ip) ->
    <<
        ?T_I32,    1:16, Ip:32/signed-integer,
        ?T_I16,    2:16, Port:16/signed-integer,
        ?T_STRING, 3:16, (byte_size(Service)):32, Service/binary,
        0
    >>;

encode_host({Service, Ip, Port}, Cfg) ->
    encode_host({Service, ip_to_i32(Ip), Port}, Cfg).



%% @private
encode_log({Timestamp, Text, undefined}, Cfg) ->
    do_encode_log(Timestamp, Text, Cfg#conf.server_bin_log_undef);

encode_log({Timestamp, Text, default}, Cfg) ->
    do_encode_log(Timestamp, Text, Cfg#conf.server_bin_log_default);

encode_log({Timestamp, Text, Service}, Cfg) ->
    SrvBin = encode_service(Service, log, Cfg),
    do_encode_log(Timestamp, Text, SrvBin).


%% @private
do_encode_log(Timestamp, Text, Service) ->
    Text2 = to_value(Text),
    <<
        ?T_I64,    1:16, Timestamp:64/signed-integer,
        ?T_STRING, 2:16, (byte_size(Text2)):32, Text2/binary,
        Service/binary,
        0
    >>.


%% @private
encode_tag({Key, {Value, undefined}}, Cfg) ->
    do_encode_tag(Key, Value, Cfg#conf.server_bin_tag_undef);

encode_tag({Key, {Value, default}}, Cfg) ->
    do_encode_tag(Key, Value, Cfg#conf.server_bin_tag_default);

encode_tag({Key, {Value, Service}}, Cfg) ->
    SrvBin = encode_service(Service, tag, Cfg),
    do_encode_tag(Key, Value, SrvBin).


%% @private
do_encode_tag(Key, Value, Service) ->
    Key2 = to_bin(Key),
    Value2 = to_value(Value),
    <<
        ?T_STRING, 1:16, (byte_size(Key2)):32, Key2/binary,
        ?T_STRING, 2:16, (byte_size(Value2)):32, Value2/binary,
        ?T_I32,    3:16, 6:32/signed-integer,
        Service/binary,
        0
    >>.


%% @private
encode_service(Service, log, Cfg) ->
    HostBin = encode_host(Service, Cfg),
    <<?T_STRUCT, 3:16, HostBin/binary>>;

encode_service(Service, tag, Cfg) ->
    HostBin = encode_host(Service, Cfg),
    <<?T_STRUCT, 4:16, HostBin/binary>>.


%% @private
to_bin(K) when is_binary(K) -> K;
to_bin(K) -> nklib_util:to_binary(K).


%% @private
to_value({Fmt, Str}) when is_list(Fmt), is_list(Str) ->
    list_to_binary(io_lib:format(Fmt, Str));
to_value(Val) when is_binary(Val) ->
    Val;
to_value(Val) when is_list(Val) ->
    list_to_binary(Val);
to_value(Val) ->
    to_value({"~p", [Val]}).


%% @private
ip_to_i32({A, B, C, D}) ->
    <<Ip:32>> = <<A, B, C, D>>,
    Ip.


%%%% @private
%%i32_to_ip(<<A, B, C, D>>) ->
%%    {A, B, C, D}.
