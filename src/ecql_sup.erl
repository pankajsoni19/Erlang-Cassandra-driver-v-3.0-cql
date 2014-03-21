
-module(ecql_sup).

-behaviour(supervisor).

-include("ecql.hrl").


%% API
-export([add_pid/2, remove_pid/2, get_pool_size/1, get_round_robin_pid/1]).

%% API
-export([ q/1
        , q/2
        , q/3
        , q/4
    ]).

%% Supervisor callbacks
-export([init/1, start_link/1]).

-record(cass_pool, {sync, pid}).

%% ===================================================================
%% API functions
%% ===================================================================
%% ["192.168.1.48",9042,"verbus","verbus",10,10,true]

start_link([Host,Port, UserName, Password, PoolSize,StartInterval,false]) ->
	{error, not_supported_in_this_version};
	
start_link([Host,Port, User, Pass, PoolSize,StartInterval,Sync]) ->
	UserName = erlang:list_to_binary(User), 
	Password = erlang:list_to_binary(Pass),
    mnesia:create_table(cass_pool,
			[{ram_copies, [node()]}, {type, bag},
			 {local_content, true},
			 {attributes, record_info(fields, cass_pool)}]),
	ets:new(cass_pool_counter, [named_table, public]),
	ets:insert(cass_pool_counter, {cass_pool, 1}),
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Host,Port, UserName, Password, PoolSize,StartInterval,Sync]).

q(Query) -> q(Query, <<2>> , one, ?CASS_QUERY_DEF_TIMEOUT).

q(Query, Flags) -> q(Query, Flags, one, ?CASS_QUERY_DEF_TIMEOUT).

q(Query, Flags, ConsistencyLevel) ->  q(Query, Flags, ConsistencyLevel, ?CASS_QUERY_DEF_TIMEOUT).
    
q(Query, Flags, ConsistencyLevel,Timeout) ->
	 Pid = get_round_robin_pid(?SYNC),
    (?GEN_FSM):sync_send_event(Pid, {q, Query, Flags, ConsistencyLevel},Timeout).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([Host,Port, UserName, Password, PoolSize,StartInterval,true]) ->
    {ok,
     {{one_for_one, PoolSize * 10, 1},
      lists:map(fun (I) ->
			{I,
			 {ecql_connection_sync, start_link,
			  [Host,Port, UserName, Password, PoolSize, StartInterval * 1000 ,I]},
			 transient, 2000, worker, [?MODULE]}
		end,
		lists:seq(1, PoolSize))}}.

get_round_robin_pid(Key) ->
	Rs = mnesia:dirty_read(cass_pool, Key),
    Pids = [R#cass_pool.pid || R <- Rs],
   	Size = length(Pids),
    lists:nth(ets:update_counter(cass_pool_counter, cass_pool, {2, 1, Size, 1}), Pids).

get_pool_size(Key) ->
	Rs = mnesia:dirty_read(cass_pool, Key),
   	Size = length(Rs).

add_pid(Key, Pid) ->
    F = fun () ->
		mnesia:write(#cass_pool{sync = Key, pid = Pid})
	end,
    mnesia:ets(F).

remove_pid(Key, Pid) ->
    F = fun () ->
		mnesia:delete_object(#cass_pool{sync = Key, pid = Pid})
	end,
    mnesia:ets(F).	
