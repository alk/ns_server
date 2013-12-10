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

-export([create_self_signed_cert/2]).

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
