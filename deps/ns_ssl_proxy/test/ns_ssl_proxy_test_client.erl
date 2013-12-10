-module(ns_ssl_proxy_test_client).

-export([test/1, cert/1]).

cert(CertDir) ->
    ns_ssl:create_self_signed_cert("/usr/bin/openssl", CertDir).

test(CertDir) ->
    {ok, Socket} = gen_tcp:connect("127.0.0.1", 9998,
                                   [binary,
                                    {reuseaddr, true},
                                    inet,
                                    {packet, raw},
                                    {active, false}], infinity),

    CertFile = filename:join([CertDir, "cert.pem"]),

    ns_ssl:establish_ssl_proxy_connection(Socket, "127.0.0.1", 80, 9997, CertFile),

    send_get(Socket, "/"),
    receive_loop(Socket).

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
