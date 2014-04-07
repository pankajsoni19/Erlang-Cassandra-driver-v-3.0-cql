-module(ecql_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
	application:start(mnesia),
	application:start(sasl),
	application:start(ecql).

start(_StartType, StartArgs) ->
    ecql_sup:start_link(StartArgs).

stop(_State) ->
   ecql_sup:stop(),
   ok.
