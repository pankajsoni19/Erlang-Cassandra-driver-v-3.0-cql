-module(ecql_connection_sync).

-include("ecql.hrl").
-include("logger.hrl").

-behaviour(?GEN_FSM).

%%%----------------------------------------------------------------------
%%% File    : ejabberd_cassandra.erl
%%% Author  : Pankaj Soni <pankajsoni@softwarejoint.com>
%%% Purpose : Serve cassandra connection
%%% Created : 6 Dec 2013 by Pankaj Soni <pankajsoni@softwarejoint.com>
%%%----------------------------------------------------------------------


%% supervisor callbacks

-export([start_link/7]).

%% gen_fsm callbacks
-export([init/1
        , handle_event/3
        , handle_sync_event/4
        , handle_info/3
        , terminate/3
        , print_state/1
    	, code_change/4]).

%% gen_fsm states
-export([connecting/2, connecting/3, session_established/3]).

-define(SUP,ecql_sup).

-record(state, {
            host,
            sock,
			port,
			username,
			password,
            caller,
            buffer = <<>>,
			pending_requests,
			start_interval,
			proc_index}).

%%%=================================
%%% supervisor callbacks
%%%==================================

start_link(Host, Port, UserName, Password, PoolSize, StartInterval ,I) ->
    (?GEN_FSM):start_link(?MODULE,
			  [Host, Port, UserName, Password, PoolSize, StartInterval ,I],[]).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------



%%    [Server, Port, User, Pass, _PoolSize, SyncMode] = db_opts(Host),
connecting({reconnect, Rows}, #state{host = Host, port = Port} = State) ->
	case catch lists:foldl(fun([{_,RpcAddr}], Acc) ->  [RpcAddr|Acc] end, [], Rows) of
			PeerList when is_list(PeerList), erlang:length(PeerList) > 1 ->
	        PoolSize = ecql_sup:get_pool_size(true),
			gen_tcp:close(State#state.sock),	
			Server = 
				if PeerList >= PoolSize -> 
						lists:nth(State#state.proc_index, PeerList);
					true ->		
						ConnectionPerNode = ceiling(PoolSize / PeerList),
						Index = ceiling(State#state.proc_index / ConnectionPerNode),
						lists:nth(Index, PeerList)
				end,
				do_connection(?RES_DISCOVER_STREAMID,PeerList,State);
		_ ->
		    check_state(?RES_DISCOVER_STREAMID, State)
	end;

connecting(discover, State) ->
	Query = <<"select rpc_address from system.peers">>,
	F = ecql_parser:make_query_frame(Query, <<0>>, one,?RES_STREAMID_START),
	case sock_send(State#state.sock, F) of
		error ->
				(?GEN_FSM):send_event_after(State#state.start_interval,connect),
				{next_state, connecting, do_close(State)};
		ok ->
				{next_state, connecting, State}
	end; 

connecting(connect, #state{host = Host} = State) ->
    ?INFO_MSG("Starting cassandra connection in sync mode: ~p ~n",[true]),
	do_connection(?RES_STREAMID_START,State);
        
connecting(Event, State) ->
    ?WARNING_MSG("unexpected event in 'connecting': ~p",
		 [Event]),
    {next_state, connecting, State}.       
     
connecting(Request, {Who, _Ref}, State) ->
    ?WARNING_MSG("unexpected call ~p from ~p in 'connecting'",[Request, Who]),
    {reply, {error, badarg}, connecting, State}.   

session_established({q, Query, Flags, ConsistencyLevel}, From, State = #state{sock=Sock}) ->
    ?DEBUG("session sync request ~p ~n",[{q, Query, Flags, ConsistencyLevel, From, State}]),
    case queue:len(State#state.pending_requests) of      
        0 when State#state.caller =:= undefined ->  
            F = ecql_parser:make_query_frame(Query, Flags, ConsistencyLevel,0),
            case sock_send(Sock, F) of
                    error ->
                        (?GEN_FSM):send_event_after(State#state.start_interval,connect),
                        {reply, {error,sock_error}, connecting, do_close(State)};
                    ok ->
                        {next_state, session_established, State#state{caller=From}}
            end; 
        Len when Len < ?MAX_Q_LEN ->
                Qin =  queue:in({q, Query, Flags, ConsistencyLevel, From} , State#state.pending_requests),
                {next_state, session_established, State#state{pending_requests = Qin}};
        _ ->    
                {reply, {error, queue_full}, session_established, State}
    end;
    
session_established(Request, From, State) ->
   ?INFO_MSG("unexpected call ~p from ~p in 'session_established'", [Request, From]),
    {reply, {error, badarg}, session_established, State}.

%%--------------------------------------------------------------------   
%%% gen fsm   
%%--------------------------------------------------------------------   

init([Host,Port, UserName, Password, PoolSize,StartInterval,I])  ->
     ?INFO_MSG("Cassandra connection can be only, async or sync mode. To switch modes restart with new settings.~n",[]),
     if I > 0 -> 
            Interval = I * 1000,
            (?GEN_FSM):send_event_after(Interval,connect);
     true ->
             (?GEN_FSM):send_event(self(), connect)
     end,  
    State = #state{ host=Host,
					port = Port,
					username = UserName,
					password = Password,
                	start_interval = StartInterval,
                	proc_index = I},
    {ok,connecting,State}.
    
handle_event(_Event, StateName, State) ->
        {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, badarg}, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------

handle_info({tcp, Sock, Data}, St, State = #state{sock=Sock, buffer=Buffer}) ->
    case ecql_parser:read_frame(<< Buffer/binary, Data/binary >>) of
        {continue, NewBuf} ->
            inet:setopts(Sock, [{active, once}]),
            {next_state, St, State#state{buffer=NewBuf}};
        {F=#frame{}, NewBuf} ->
            %io:format("got frame: ~p\n",[F]),
            inet:setopts(Sock, [{active, once}]),
            handle_frame(St, F, State#state{buffer=NewBuf})
    end;

handle_info({tcp_closed, _Sock}, _St, State) ->
    ?DEBUG("tcp_closed socket. ~p ~n",[State]),    
    ?SUP:remove_pid(?SYNC, self()),
    (?GEN_FSM):send_event(self(), connect),
    {next_state,connecting,State#state{sock=undefined}};
        
handle_info({tcp_error, _Sock, Reason}, _St, State) ->
    ?DEBUG("tcp_closed socket..~p ~n ~p ~n",[Reason,State]),    
    (?GEN_FSM):send_event(self(), connect),
    {next_state,connecting,do_close(State)};
    
handle_info(Info, StateName, State) ->
    io:format("Unhandled info when ~p ~p\n",[StateName, Info]),
    {next_state, StateName, State}.

terminate(_Reason, _StateName, State) ->
        ?DEBUG("Terminating cassandra connection for ~p",[_Reason]),
        do_close(State),
        exit(normal).

print_state(State) -> State.

code_change(_OldVsn, StateName, State, _Extra) ->
        {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

       

do_connection(?RES_STREAMID_START,State) ->
	do_connection(?RES_STREAMID_START, undefined, State, 1).

do_connection(?RES_DISCOVER_STREAMID,PeerList,State) ->
	do_connection(?RES_DISCOVER_STREAMID, PeerList, State, ?MAX_TRY).

do_connection(_StreamId, _PeerList, State, 0) ->
	(?GEN_FSM):send_event_after(State#state.start_interval,connect),
	{next_state, connecting, State};

do_connection(StreamId, PeerList, State, Tries) ->	
	Server = State#state.host,
	Port = State#state.port,
	case connect_sock(Server, Port) of
	{error, _Reason} ->		
		check_error_type(Server, Port, StreamId, PeerList, State, Tries),
		{next_state, connecting, State};
	{ok, Sock} ->        
		StartupFrame = #frame{
				opcode = ?OP_STARTUP,
				stream = StreamId,
				body = ecql_parser:encode_string_map([{<<"CQL_VERSION">>,?CQL_VERSION}])
		},                
		case sock_send(Sock, StartupFrame) of
			error ->
				gen_tcp:close(Sock),
				check_error_type(Server, Port, StreamId, PeerList, State, Tries);
			ok ->
				{next_state, connecting, State#state{sock = Sock}}
		end
	end.

check_error_type(Server, Port, StreamId, PeerList, State, Tries) ->
	?ERROR_MSG("Connection to cassandra node failed ~p ~n",[{Server, Port, StreamId, PeerList, State, Tries}]),
	case StreamId of
		?RES_DISCOVER_STREAMID ->
			Pl = lists:subtract(PeerList, Server),
			Sip = lists:nth(erlang:phash(now(), length(Pl)), Pl),
			do_connection(StreamId, Pl, State, Tries-1);
		?RES_STREAMID_START->
			do_connection(StreamId, PeerList, State, Tries-1)
	end.

ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

update_ordsets_element(StreamId,State) ->
       Data = State#state.pending_requests,
       case ordsets:fold(fun({Ix,Pd},Acc) -> 
                    case StreamId of
                       Ix ->
                           {Ix,Pd};     
                       _ -> 
                          Acc      
                    end    
                end,{},Data) of
              {} -> error;
              Element ->              
                     {Element, ordsets:del_element(Element,Data)}
       end.
       
get_async_index(State) ->
        Data = State#state.pending_requests,
        PendingReq = ordsets:size(Data),
        if PendingReq < ?MAX_Q_LEN ->
         {ok,Ix} = ordsets:fold(fun({Num, _},{_, Acc}) -> 
                                case Acc of 
                                        Num -> Acc + 1; 
                                        Acc when is_integer(Acc) -> {ok,Acc}; 
                                        _ -> Acc 
                                end 
                             end,{ok, 1},Data),
                   Ix;              
        true ->
               queue_full
        end.

do_close(State) ->
	?SUP:remove_pid(?SYNC, self()),
	gen_tcp:close(State#state.sock),
	queue:filter(fun({q, _Query, _Flags, _ConsistencyLevel, From}) ->
            		           send_reply(From, {error,sock_error}), 
	                           false
	                        end, State#state.pending_requests),
	State#state{sock = undefined, pending_requests = undefined, caller = undefined}.

sock_send(Sock, F=#frame{}) ->
    %io:format("sock_send: ~p\n",[F]),
    Enc = ecql_parser:encode(F),
    %io:format("SEND: ~p\n",[iolist_to_binary(Enc)]),
    case gen_tcp:send(Sock, Enc) of
            {error,timeout} -> ok;
            {error,_Reason} -> error;
            ok -> ok
    end.

parse_error_body(<< Code:?int,Body/binary >>) ->
    case ecql_parser:error_code(Code) of
        Atom when is_atom(Atom) ->
            Atom;
        {Atom, BodyParser} when is_atom(Atom), is_function(BodyParser,1) ->
            {Atom, BodyParser(Body)}
    end.
      
%%{next_state, NextStateName, NextState}         

%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%% Auth Frames
%%%%%%%%%%%%%%%

handle_frame(connecting, #frame{opcode=?OP_AUTH_CHALLENGE, stream=StreadId, body=Body}, State = #state{sock=Sock}) ->
	{ChallangeBody,_} = ecql_parser:consume_string(Body),
	AutoResponse = <<"yyyy">>,
	?DEBUG("GIVE CHALLANGE Response to ... ChallangeBody: ~p ~nCurrently sending ~p ~n",[ChallangeBody,AutoResponse]),
	CredBody = ecql_parser:encode_long_string(<<"yyyy">>),
	CredF =	#frame{
		opcode=?OP_AUTH_RESPONSE,
		stream=StreadId,
		body=CredBody
	},
	case sock_send(Sock, CredF) of
		error ->
			(?GEN_FSM):send_event_after(State#state.start_interval,connect),
			{next_state, connecting, do_close(State)};
		ok ->
			{next_state, connecting, State}
	end;

handle_frame(connecting, #frame{opcode=?OP_AUTH_SUCCESS, stream=StreadId, body=_Body}, State) ->
    ?DEBUG("Final auth body ~p ~n",[_Body]),
    check_state(StreadId,State);

handle_frame(connecting, #frame{opcode=?OP_READY, stream=StreadId}, State) ->
    check_state(StreadId,State);
   
handle_frame(connecting, #frame{opcode=?OP_AUTHENTICATE, stream=StreadId, body=Body}, State = #state{sock=Sock}) ->
        {IAuthenticator,_} = ecql_parser:consume_string(Body),
        ?DEBUG("Asked to authenticate using: ~s\n",[IAuthenticator]),
        User = State#state.username,
		Pass = State#state.password,
		CredBody = ecql_parser:encode_long_string(<<<<"\0">>/binary, User/binary ,<<"\0">>/binary ,  Pass/binary>>),
		CredF =	#frame{
						opcode=?OP_AUTH_RESPONSE,
						body=CredBody,
						stream=StreadId
					},		
		case sock_send(Sock, CredF) of
			error ->
				(?GEN_FSM):send_event_after(State#state.start_interval,connect),
				{next_state, connecting, do_close(State)};
			ok ->
				{next_state, connecting, State}
		end;

handle_frame(connecting, #frame{opcode=?OP_ERROR, body=Body}, State = #state{sock=Sock}) ->
            gen_tcp:close(Sock),    
            ?INFO_MSG("Cassandra connection failed:~n** Reason: ~p~n** "
		    "Retry after: ~p seconds",  [parse_error_body(Body),  State#state.start_interval div 1000]),
			(?GEN_FSM):send_event_after(State#state.start_interval, connect),
            {next_state, connecting, State#state{sock = undefined}};

%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%% Message Frames
%%%%%%%%%%%%%%%

handle_frame(StateName, #frame{opcode=?OP_ERROR, stream = StreamId, body = Body}, State) ->
    Reply = {error, parse_error_body(Body)},
    {next_state, StateName, send_reply(Reply, State,StreamId)};

handle_frame(_StateName, #frame{opcode=?OP_RESULT, stream = StreamId, body = <<Kind:?int,Body/binary>> }, State) ->
    case Kind of
        1 -> %%   Void: for results carrying no information.
            {next_state, session_established, send_reply(ok, State,StreamId)};
        2 -> %%   Rows: for results to select queries, returning a set of rows.
            {Metadata, Rest1} = ecql_parser:consume_metadata(Body),
            {NumRows, Rest2}  = ecql_parser:consume_int(Rest1),
            {Rows, _}         = ecql_parser:consume_num_rows(NumRows, Metadata, Rest2),
			?DEBUG("Data recieved for query metadata: ~p ~n",[Metadata]),
			?DEBUG("Data recieved for query rows: ~p ~n",[Rows]),
            Reply = {rows, Metadata, Rows},
			case StreamId of
				?RES_STREAMID_START ->
					(?GEN_FSM):send_event(self(), {reconnect, Rows}),
					{next_state, connecting, State};
				_ ->
					{next_state, session_established, send_reply(Reply, State,StreamId)}
			end;            
        3 -> %%   Set_keyspace: the result to a `use` query.
            {KS,_} = ecql_parser:consume_string(Body),
            Reply = {ok, KS},
            {next_state, session_established, send_reply(Reply, State,StreamId)};
        4 -> %%   Prepared: result to a PREPARE message.
            {PrepID, Rest} = ecql_parser:consume_short_bytes(Body),
            {Metadata, _} = ecql_parser:consume_metadata(Rest),
            Reply = {PrepID, Metadata},
            {next_state, session_established, send_reply(Reply, State,StreamId)};
        5 -> %%   Schema_change: the result to a schema altering query.
          % - <change> describe the type of change that has occured. It can be one of
          %   "CREATED", "UPDATED" or "DROPPED".
          % - <keyspace> is the name of the affected keyspace or the keyspace of the
          %   affected table.
          % - <table> is the name of the affected table. <table> will be empty (i.e.
          %   the empty string "") if the change was affecting a keyspace and not a
          %   table.
            {Change, Rest1}   = ecql_parser:consume_string(Body),
            {KeySpace, Rest2} = ecql_parser:consume_string(Rest1),
            {Table, _Rest3}   = ecql_parser:consume_string(Rest2),
            Reply = {Change, KeySpace, Table},
            {next_state, session_established, send_reply(Reply, State,StreamId)}
    end;

handle_frame(_St, F, State) ->
    ?DEBUG("unhandled frame recieved. is sock erred??? ~p ~n",[F]),
    (?GEN_FSM):send_event_after(State#state.start_interval, connect),
    {next_state,connecting,do_close(State)}.

check_state(StreamId, State) ->
    case StreamId of
	    ?RES_STREAMID_START ->
		    (?GEN_FSM):send_event(self(), discover),
		    {next_state, connecting, State};
	    ?RES_DISCOVER_STREAMID ->
		    ?SUP:add_pid(?SYNC, self()),
			{next_state, session_established,State#state{pending_requests = queue:new()}};
		_ -> 
		    ?ERROR_MSG("Stream Id got: ~p ~n for state: ~p ~n", [StreamId, State]),
			(?GEN_FSM):send_event_after(State#state.start_interval,connect),
		    {next_state,connecting,do_close(State)}		     
    end.

send_reply(From,Reply) when is_pid(From)->
    catch From ! Reply;
        
send_reply({From, Tag},Reply) when is_pid(From) ->
    catch From ! {Tag, Reply}.

send_reply(Reply, State ,StreamId) ->
    (?GEN_FSM):reply(State#state.caller, Reply),
            check_queue(State).

check_queue(State) ->  
    case queue:len(State#state.pending_requests) of      
        0 ->
            State#state{caller=undefined};
        Len when is_integer(Len) ->
            {{value, {q, Query, Flags, ConsistencyLevel, From}}, Q2} = queue:out(State#state.pending_requests),
            F = ecql_parser:make_query_frame(Query, Flags, ConsistencyLevel,0),
            case sock_send(State#state.sock, F) of
                    error ->
                        (?GEN_FSM):send_event_after(State#state.start_interval,connect),
                        {reply, {error,sock_error}, connecting, do_close(State)};
                    ok ->
                        {next_state, session_established, State#state{caller=From, pending_requests = Q2 }}
            end;
        _ ->    
            State#state{caller=undefined}
    end.

connect_sock(Server,Port) when is_binary(Server) ->
        connect_sock(binary_to_list(Server),Port);

connect_sock(Server,Port) when is_list(Server) ->
    Opts = [
        {active, once},
        {packet, raw},
        binary,
        {nodelay, true}
    ],
    gen_tcp:connect(Server, Port, Opts,?GEN_TCP_CONNECT_TIMEOUT).