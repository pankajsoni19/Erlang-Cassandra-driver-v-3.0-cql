
-module(ecql_sup).

-behaviour(supervisor).

-include("ecql.hrl").
-include("logger.hrl").

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

-export([stop/0]).

-record(cass_pool, {sync, pid}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link([_Host, _Port, _UserName, _Password, _PoolSize, _StartInterval, false]) ->
	{error, not_supported_in_this_version};
	
start_link([Host,Port, User, Pass, PoolSize,StartInterval,Sync]) ->
	UserName = erlang:list_to_binary(User), 
	Password = erlang:list_to_binary(Pass),
	mnesia:create_table(cass_pool,
		[{ram_copies, [node()]}, {type, bag},
		 {local_content, true},
		 {attributes, record_info(fields, cass_pool)}]),
	case ets:info(cass_pool_counter) of
		undefined ->	
		   ets:new(cass_pool_counter, [named_table, public]),
		   ets:insert(cass_pool_counter, {cass_pool, 1}),
		   supervisor:start_link({local, ?MODULE}, ?MODULE, [Host,Port, UserName, Password, PoolSize,StartInterval,Sync]);
		_ ->
		  ok
	end.

q(Query) -> q(Query, <<2>> , one, ?CASS_QUERY_DEF_TIMEOUT).

q(Query, Flags) -> q(Query, Flags, one, ?CASS_QUERY_DEF_TIMEOUT).

q(Query, Flags, ConsistencyLevel) ->  q(Query, Flags, ConsistencyLevel, ?CASS_QUERY_DEF_TIMEOUT).
    
q(Query, Flags, ConsistencyLevel,Timeout) ->
  q(Query, Flags, ConsistencyLevel,Timeout, ?MAX_DBQUERY_TRY).

q(_Query, _Flags, _ConsistencyLevel, _Timeout, 0) ->
     error;

q(Query, Flags, ConsistencyLevel, Timeout, Tries) ->
    case get_round_robin_pid(?SYNC) of
        error -> error;
	Pid ->
	    case catch (?GEN_FSM):sync_send_event(Pid, {q, Query, Flags, ConsistencyLevel},Timeout) of
		{'EXIT', Reason} ->
			?DEBUG("Cassandra query error: ~p ~n for process ~p ~n by Cassandra pid ~p ~n",[Reason, self(), Pid]),
			q(Query, Flags, ConsistencyLevel,Timeout, Tries -1);
        	{error, R} ->
			?DEBUG("Cassandra query error: ~p ~n for process ~p ~n by Cassandra pid ~p ~n",[R, self(), Pid]),
			error;
		Data ->
		    Data
	   end
    end.	

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
    case Size of
	0 -> error;
	Size ->
	    lists:nth(ets:update_counter(cass_pool_counter, cass_pool, {2, 1, Size, 1}), Pids)
    end.

get_pool_size(Key) ->
	Rs = mnesia:dirty_read(cass_pool, Key),
   	length(Rs).

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

stop() ->
 stop_sync(),
 ok. 

stop_sync() ->
  Rs = mnesia:dirty_read(cass_pool, ?SYNC),
  lists:foreach(fun(R) -> 
       gen_fsm:send_all_state_event(R#cass_pool.pid, stop_now)
   end, Rs),
   ok.
