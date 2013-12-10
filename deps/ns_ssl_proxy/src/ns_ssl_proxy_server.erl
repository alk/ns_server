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

-module (ns_ssl_proxy_server).

-export([start_link/1, init/1]).

-export([loop_plain_to_ssl/3, loop_ssl_to_plain/3]).

-define(CONTROL_PAYLOAD_TIMEOUT, 10000).
-define(PROXY_RESPONSE_TIMEOUT, 60000).

-include("ns_common.hrl").

start_link(Socket) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Socket]),
    {ok, Pid}.

init(Socket) ->
    process_flag(trap_exit, true),
    ?log_debug("Server started on socket ~p ~n", [Socket]),
    inet:setopts(Socket, [{active, false}]),
    KV = ns_ssl:receive_json(Socket, ?CONTROL_PAYLOAD_TIMEOUT),

    Port = proplists:get_value(<<"port">>, KV),
    case {proplists:get_value(<<"proxyHost">>, KV),
          proplists:get_value(<<"proxyPort">>, KV)
         } of
        {undefined, undefined} ->
            init_server_proxy(Socket, Port);
        {H, P} ->
            init_client_proxy(Socket, Port, binary_to_list(H), P)
    end.

send_initial_req(Socket, Port) ->
    ns_ssl:send_json(Socket, {[{port, Port}]}).

send_reply(Socket, Type) ->
    ns_ssl:send_json(Socket, {[{type, list_to_binary(Type)}]}).

init_client_proxy(Socket, Port, ProxyHost, ProxyPort) ->
    DerEncodedCert = ns_ssl:receive_binary_payload(Socket, ?CONTROL_PAYLOAD_TIMEOUT),
    Cert = public_key:pkix_decode_cert(DerEncodedCert, otp),

    {ok, PlainSocket} = gen_tcp:connect(ProxyHost, ProxyPort,
                                        [binary,
                                         {reuseaddr, true},
                                         inet,
                                         {packet, raw},
                                         {active, false}], ?PROXY_RESPONSE_TIMEOUT),
    send_initial_req(PlainSocket, Port),

    KV = ns_ssl:receive_json(PlainSocket, ?CONTROL_PAYLOAD_TIMEOUT),
    <<"upgrade">> = proplists:get_value(<<"type">>, KV),

    {ok, SSLSocket} = ssl:connect(PlainSocket,
                                  [{verify, verify_peer},
                                   {verify_fun, {fun verify_fun/3, Cert}},
                                   {depth, 0}]),

    send_reply(Socket, "ok"),

    proc_lib:spawn_link(?MODULE, loop_plain_to_ssl, [Socket, SSLSocket, self()]),
    proc_lib:spawn_link(?MODULE, loop_ssl_to_plain, [Socket, SSLSocket, self()]),
    wait_and_close_sockets(Socket, SSLSocket).

init_server_proxy(PlainSocket, Port) ->
    send_reply(PlainSocket, "upgrade"),

    {ok, CertFile} = application:get_env(ns_ssl_proxy, cert_file),
    {ok, PrivateKeyFile} = application:get_env(ns_ssl_proxy, private_key_file),

    {ok, SSLSocket} = ssl:ssl_accept(PlainSocket,
                                     [{certfile, CertFile},
                                      {keyfile, PrivateKeyFile}]),

    {ok, Socket} = gen_tcp:connect("127.0.0.1", Port,
                                   [binary,
                                    {reuseaddr, true},
                                    inet,
                                    {packet, raw},
                                    {active, false}], infinity),

    proc_lib:spawn_link(?MODULE, loop_plain_to_ssl, [Socket, SSLSocket, self()]),
    proc_lib:spawn_link(?MODULE, loop_ssl_to_plain, [Socket, SSLSocket, self()]),
    wait_and_close_sockets(Socket, SSLSocket).

wait_and_close_sockets(PlainSocket, SSLSocket) ->
    receive
        {{error, closed}, PlainSocket} ->
            ?log_debug("Connection closed on socket ~p~n", [PlainSocket]),
            ssl:close(SSLSocket);
        {{error, closed}, SSLSocket} ->
            ?log_debug("Connection closed on socket ~p~n", [SSLSocket]),
            gen_tcp:close(PlainSocket);
        {{error, Reason}, Sock} ->
            ?log_error("Error ~p on socket ~p~n", [Reason, Sock]),
            ssl:close(SSLSocket),
            gen_tcp:close(PlainSocket),
            exit({error, Reason});
        {'EXIT', Pid, Reason} ->
            ?log_debug("Subprocess ~p exited with reason ~p~n", [Pid, Reason]),
            ssl:close(SSLSocket),
            gen_tcp:close(PlainSocket)
    end.

loop_plain_to_ssl(PlainSock, SSLSock, Parent) ->
    case gen_tcp:recv(PlainSock, 0) of
        {ok, Packet} ->
            case ssl:send(SSLSock, Packet) of
                ok ->
                    loop_plain_to_ssl(PlainSock, SSLSock, Parent);
                Error ->
                    Parent ! {Error, SSLSock}
            end;
        Error ->
            Parent ! {Error, PlainSock}
    end.

loop_ssl_to_plain(PlainSock, SSLSock, Parent) ->
    case ssl:recv(SSLSock, 0) of
        {ok, Packet} ->
            case gen_tcp:send(PlainSock, Packet) of
                ok ->
                    loop_ssl_to_plain(PlainSock, SSLSock, Parent);
                Error ->
                    Parent ! {Error, PlainSock}
            end;
        Error ->
            Parent ! {Error, SSLSock}
    end.

verify_fun(Cert, Event, Etalon) ->
    Resp = case Event of
               {bad_cert, selfsigned_peer} ->
                   case Cert of
                       Etalon ->
                           {valid, Etalon};
                       _ ->
                           {fail, {bad_cert, unrecognized}}
                   end;
               {bad_cert, _} ->
                   {fail, Event};
               {extension, _} ->
                   {unknown, Event};
               valid ->
                   {fail, {bad_cert, unrecognized}};
               valid_peer ->
                   {fail, {bad_cert, unrecognized}}
           end,
    case Resp of
        {valid, _} ->
            Resp;
        _ ->
            ?log_error("Certificate validation failed with reason ~p~n", [Resp]),
            Resp
    end.
