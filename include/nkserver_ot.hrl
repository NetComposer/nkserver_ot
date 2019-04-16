-ifndef(NKSERVER_SPAN_HRL_).
-define(NKSERVER_SPAN_HRL_, 1).

%% ===================================================================
%% Defines
%% ===================================================================

%% ===================================================================
%% Records
%% ===================================================================



-record(span, {
    srv :: nkserver:id(),
    timestamp  :: nkserver_ot:time(),
    trace_id   :: nkserver_ot:trace_code() | undefined,
    name       :: nkserver_ot:name(),
    id         :: nkserver_ot:span_code() | undefined,
    parent_id  :: nkserver_ot:span_code() | undefined,
    tags = #{} :: #{binary() => binary()|integer()},
    logs = []  :: [{nkserver_ot:time(), binary()}],
    duration   :: nkserver_ot:time()
}).


-endif.

