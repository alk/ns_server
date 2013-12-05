%% @author Couchbase <info@couchbase.com>
%% @copyright 2013 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(ns_ssl_proxy_listener).

-behavior(gen_server).

%% API
-export([start_link/0, stop/0, listen/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

start_link() ->
   gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
   gen_server:call(?SERVER, stop).

init([]) ->
    {ok, Port} = application:get_env(ns_ssl_proxy, port),

    case gen_tcp:listen(Port,
                        [binary,
                         {reuseaddr, true},
                         inet,
                         {ip, {127, 0, 0, 1}},
                         {packet, raw},
                         {active, false}]) of
        {ok, Sock} ->
            spawn_link(?MODULE, listen, [Sock]),
            {ok, Sock};
        {error, Reason} ->
            {stop, Reason}
    end.

listen(LSock) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            Pid = ns_ssl_proxy_server_sup:start_server(Sock),
            ok = gen_tcp:controlling_process(Sock, Pid),
            listen(LSock);
        {error, closed} ->
            exit(shutdown);
        Error ->
            exit(Error)
    end.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call(_Request, _From, State) ->
    {reply, refused, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, Sock) ->
    gen_tcp:close(Sock),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
