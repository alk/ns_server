-module(proxied_connections_pool).

%% -export([start_link/1]).

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        code_change/3,
        terminate/2
    ]).

-export([establish_proxied_connection/4]).

-behaviour(gen_server).

-record(state, {pool}).

init({Pool}) ->
    Tid = ets:new(none, []),
    erlang:put(host_port_to_proxy_tid, Tid),
    {ok, #state{pool = Pool}}.

handle_call({socket, _Pid, {Host, Port, _Bucket}} = CallMsg, _From,
            #state{pool = Pool} = State) ->
    %% TODO: process and blocking
    case gen_server:call(Pool, CallMsg, infinity) of
        {ok, Socket} ->
            {reply, {ok, Socket}, State};
        no_socket ->
            do_connect(Host, Port)
    end.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_connect(Host, Port) ->
    Tid = erlang:get(host_port_to_proxy_tid),
    case ets:lookup(Tid, {Host, Port}) of
        [] ->
            %% TODO
            erlang:error(crap);
        [{_,  LocalPort, RemotePort}] ->
            establish_proxied_connection(LocalPort, RemotePort, Host, Port)
    end.

establish_proxied_connection(LocalPort, RemotePort, Host, Port) ->
   {ok, Socket} = gen_tcp:connections("127.0.0.1", LocalPort),
    ReqPayload = ejson:encode({[{host, <<"127.0.0.1">>},
                                {port, Port},
                                {proxyHost, list_to_binary(Host)},
                                {proxyPort, RemotePort}]}),
    FullReqPayload = [<<(erlang:size(ReqPayload)):32/big>> | ReqPayload],
    ok = gen_tcp:send(Socket, FullReqPayload),
    {ok, ReplSize} = gen_tcp:recv(Socket, 4),
    {ok, RepBinPayload} = gen_tcp:recv(Socket, ReplSize),
    {KV} = ejson:decode(RepBinPayload),
    case proplists:get_value(<<"type">>, KV) of
        <<"ok">> ->
            {ok, Socket};
        _ ->
            erlang:error('TODO')
    end.
