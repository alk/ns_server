%% @author Couchbase, Inc <info@couchbase.com>
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
%%

-module(ns_rebalance_observer).

-behavior(gen_server).

-include("ns_common.hrl").

-export([start_link/0]).

%% gen_server callbacks
-export([code_change/3, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-record(replica_building_stats, {node :: node(),
                                 docs_total :: non_neg_integer(),
                                 docs_left :: non_neg_integer()}).

-record(move_state, {vbucket :: vbucket_id(),
                     before_chain :: [node()],
                     after_chain :: [node()],
                     vbucket_to_docs_count :: dict(),
                     stats :: [#replica_building_stats{}]}).

-record(state, {bucket :: bucket_name() | undefined,
                current_moves :: [#move_state{}]
               }).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #state{bucket = undefined,
                current_moves = []}}.

handle_call(Req, From, State) ->
    ?log_error("Got unknown request: ~p from ~p", [Req, From]),
    {reply, unknown_request, State}.

handle_cast(Req, State) ->
    ?log_error("Got unknown cast: ~p", [Req]),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
