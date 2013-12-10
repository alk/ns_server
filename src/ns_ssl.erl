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

-module(ns_ssl).

-export([create_self_signed_cert/2, establish_ssl_proxy_connection/5]).

-export([receive_json/2, receive_binary_payload/2, send_json/2]).

-include("ns_common.hrl").

-define(MAX_PAYLOAD_SIZE, 10000).

create_self_signed_cert(OpensslPath, Dir) ->
    CnfFile = filename:join([Dir, "req.cnf"]),
    file:write_file(CnfFile, req_cnf("NSServer")),

    KeyFile = filename:join([Dir, "privkey.pem"]),
    CertFile = filename:join([Dir, "cert.pem"]),
    Cmd = [OpensslPath ++ " req"
           " -new"
           " -x509"
           " -config ", CnfFile,
           " -keyout ", KeyFile,
           " -out ", CertFile],
    cmd(Cmd),
    file:delete(CnfFile),
    {KeyFile, CertFile}.

req_cnf(Name) ->
    ["[req]\n"
     "default_bits       = 2048\n"
     "encrypt_key        = no\n"
     "default_md         = sha1\n"
     "prompt             = no\n"
     "distinguished_name = name\n"
     "\n"

     "[name]\n"
     "commonName = ", Name, "\n"].

cmd(Cmd) ->
    FCmd = lists:flatten(Cmd),
    Port = open_port({spawn, FCmd}, [stream, eof, exit_status]),
    eval_cmd(Port).

eval_cmd(Port) ->
    receive
        {Port, {data, _}} ->
            eval_cmd(Port);
        {Port, eof} ->
            ok
    end,
    receive
        {Port, {exit_status, Status}} when Status /= 0 ->
            exit(Status)
    after 0 ->
            ok
    end.

establish_ssl_proxy_connection(Socket, Host, Port, ProxyPort, CertFile) ->
    send_json(Socket, {[{proxyHost, list_to_binary(Host)},
                        {proxyPort, ProxyPort},
                        {port, Port}]}),
    send_cert(Socket, CertFile),
    Reply = receive_json(Socket, infinity),
    <<"ok">> = proplists:get_value(<<"type">>, Reply).

send_cert(Socket, CertFile) ->
    {ok, PemBin} = file:read_file(CertFile),
    [{'Certificate', DerEncoded, _}] = public_key:pem_decode(PemBin),

    ok = gen_tcp:send(Socket, [<<(erlang:size(DerEncoded)):32/big>> | DerEncoded]).

receive_binary_payload(Socket, Timeout) ->
    {ok, <<Size:32>>} = gen_tcp:recv(Socket, 4, Timeout),
    if
        Size > ?MAX_PAYLOAD_SIZE ->
            ?log_error("Received invalid payload size ~p~n", [Size]),
            throw(invalid_size);
        true ->
            ok
    end,
    {ok, Payload} = gen_tcp:recv(Socket, Size, Timeout),
    Payload.

receive_json(Socket, Timeout) ->
    {KV} = ejson:decode(receive_binary_payload(Socket, Timeout)),
    KV.

send_json(Socket, Json) ->
    ReqPayload = ejson:encode(Json),
    FullReqPayload = [<<(erlang:size(ReqPayload)):32/big>> | ReqPayload],
    ok = gen_tcp:send(Socket, FullReqPayload).
