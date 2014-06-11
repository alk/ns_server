-module(xdcr_trace_log_formatter).

-export([format_msg/2]).

-include_lib("ale/include/ale.hrl").

format_msg(#log_info{user_data = {Pid, Type, KV},
                     module = M,
                     function = F,
                     line = L,
                     time = TS}, []) ->
    KV1 = [case V of
               _ when is_list(V) -> {K, list_to_binary(V)};
               {json, RealV} -> {K, RealV};
               _ when is_pid(V) -> {K, xdcr_trace:format_pid(V)};
               _ -> {K, V}
           end || {K, V} <- KV,
                  V =/= undefined],

    Loc = io_lib:format("~s:~s:~B", [M, F, L]),

    JSON = ejson:encode({[{pid, list_to_binary(erlang:pid_to_list(Pid))},
                          {type, Type},
                          {ts, misc:time_to_epoch_float(TS)}
                          | KV1] ++ [{loc, iolist_to_binary(Loc)}]}),
    [JSON, $\n];
format_msg(_, _) ->
    [].
