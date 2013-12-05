-module(ns_ssl_proxy_test_client).

-export([test/0]).

test() ->
    {ok, Socket} = gen_tcp:connect("127.0.0.1", 9998,
                                   [binary,
                                    {reuseaddr, true},
                                    inet,
                                    {packet, raw},
                                    {active, false}], infinity),

    ns_ssl_proxy_server:send_json(Socket, {[{proxyHost, list_to_binary("127.0.0.1")},
                                            {proxyPort, 9997},
                                            {port, 80}]}),

    send_cert(Socket),

    Reply = ns_ssl_proxy_server:receive_json(Socket),
    io:format("Reply: ~p~n", [Reply]),

    send_get(Socket, "/"),
    receive_loop(Socket).

get_cert() ->
    {ok, PemBin} = file:read_file("cacert.pem"),
    [{'Certificate', DerEncoded, _}] = public_key:pem_decode(PemBin),
    DerEncoded.

send_cert(Socket) ->
    Cert = get_cert(),
    ok = gen_tcp:send(Socket, [<<(erlang:size(Cert)):32/big>> | Cert]).

receive_loop(Socket) ->
    case gen_tcp:recv(Socket, 0, infinity) of
        {ok, Packet} ->
            io:format("Received packet ~p~n", [Packet]),
            receive_loop(Socket);
        Error ->
            io:format("Socket closed with ~p~n", [Error])
    end.

send_get(Socket, Url) ->
    Req = "GET " ++ Url ++ " HTTP/1.1\r\nHost: 127.0.0.1:1222\r\n\r\n",
    ReqBin = list_to_binary(Req),
    ok = gen_tcp:send(Socket, ReqBin).
