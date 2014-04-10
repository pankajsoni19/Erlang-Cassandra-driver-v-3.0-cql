-define(CQL_VERSION, <<"3.0.0">>).

-define(GEN_FSM, gen_fsm).

-define(DEFAULT_PORT, 9042). %% native proto, not thrift
-define(DEFAULT_POOL_SIZE_, 10).
-define(GEN_TCP_CONNECT_TIMEOUT,10000).
-define(CASS_QUERY_DEF_TIMEOUT,10000).
-define(DEF_START_INTERVAL,10000).
-define(MAX_Q_LEN,120).
-define(RES_DISCOVER_STREAMID,124).
-define(RES_STREAMID_START,125).
-define(MAX_TRY, 5).
-define(MAX_DBQUERY_TRY, 3).

-define(SYNC, sync).
-define(ASYNC, async).

-record(frame, {
        type = 0,
        version = 2,   %% version=1=proto=1, version=2=proto=2
        flags = 0,
        stream = 0,  %%  0 sync. rest 1 to 128 async., query returned with stream_id
        opcode,
        length,
        body = <<>>
    }).

-record(metadata, {
    flags = 0,
    numcols = 0,
    columns = [],
    global_keyspace,
    global_table,
	paging_state = undefined
    }).

-define(short, 1/big-unsigned-unit:16).
-define(int,   1/big-signed-unit:32).
-define(int8,  1/big-signed-unit:8).

-define(CONSISTENCY_ANY,            0).
-define(CONSISTENCY_ONE,            1).
-define(CONSISTENCY_TWO,            2).
-define(CONSISTENCY_THREE,          3).
-define(CONSISTENCY_QUORUM,         4).
-define(CONSISTENCY_ALL,            5).
-define(CONSISTENCY_LOCAL_QUORUM,   6).
-define(CONSISTENCY_EACH_QUORUM,    7).
-define(CONSISTENCY_SERIAL,         8).
-define(CONSISTENCY_LOCAL_SERIAL,   9).

-define(OP_ERROR,       0).
-define(OP_STARTUP,     1).
-define(OP_READY,       2).
-define(OP_AUTHENTICATE,3).
-define(OP_OPTIONS,     5).
-define(OP_SUPPORTED,   6).
-define(OP_QUERY,       7).
-define(OP_RESULT,      8).
-define(OP_PREPARE,     9).
-define(OP_EXECUTE,     10).
-define(OP_REGISTER,    11).
-define(OP_EVENT,       12).
-define(OP_BATCH,       13).
-define(OP_AUTH_CHALLENGE, 14).
-define(OP_AUTH_RESPONSE,  15).
-define(OP_AUTH_SUCCESS,   16). 

-define(HAS_MORE_PAGES, 	2).
-define(NO_METADATA,		4).
-define(GLOBAL_TABLES_SPEC, 1).

-define(FLAG_COMPRESSION,   1).
-define(FLAG_TRACING,       2).