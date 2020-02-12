-ifndef(NKSERVER_SPAN_HRL_).
-define(NKSERVER_SPAN_HRL_, 1).

%% ===================================================================
%% Defines
%% ===================================================================

%% ===================================================================
%% Records
%% ===================================================================



-record(span, {
    srv        :: nkserver:id(),
    app        :: nkserver_ot:name(),
    timestamp  :: nkserver_ot:time(),
    trace_code :: nkserver_ot:trace_code() | undefined,
    name       :: nkserver_ot:name(),
    span_code :: nkserver_ot:span_code() | undefined,
    parent_code :: nkserver_ot:span_code() | undefined,
    tags = #{} :: #{binary() => binary()|integer()},
    logs = []  :: [{nkserver_ot:time(), binary()}],
    duration   :: nkserver_ot:time()
}).


-record(span_parent, {
    trace_code :: nkserver_ot:trace_code() | undefined,
    span_code :: nkserver_ot:span_code() | undefined
}).


-endif.

