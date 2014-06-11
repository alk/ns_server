%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2014 Couchbase, Inc.
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

-module(xdcr_trace).

-include("ns_common.hrl").

-export([is_enabled/0,
         is_debug_enabled/0,
         %% log/2,
         %% debug/2,
         format_pid/1,
         format_ts/1,
         format_pp/1]).

is_enabled() ->
    ale:is_loglevel_enabled(?XDCR_TRACE_LOGGER, info).

is_debug_enabled() ->
    ale:is_loglevel_enabled(?XDCR_TRACE_LOGGER, debug).

%% log(Type, KV) ->
%%     ale:xlog(?XDCR_TRACE_LOGGER, info, {self(), Type, KV}, []).

%% debug(Type, KV) ->
%%     ale:xlog(?XDCR_TRACE_LOGGER, debug, {self(), Type, KV}, []).

format_pid(Pid) ->
    erlang:list_to_binary(erlang:pid_to_list(Pid)).

format_ts(Time) ->
    misc:time_to_epoch_float(Time).

format_pp(Reason) ->
    case ale:is_loglevel_enabled(?XDCR_TRACE_LOGGER, debug) of
        true ->
            iolist_to_binary(io_lib:format("~p", [Reason]));
        _ ->
            []
    end.
