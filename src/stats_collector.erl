%% @author Northscale <info@northscale.com>
%% @copyright 2009 NorthScale, Inc.
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

-module(stats_collector).

-include_lib("eunit/include/eunit.hrl").

-include("ns_common.hrl").

-behaviour(gen_server).

-define(STATS_TIMER, 1000). % How often in ms we collect stats
-define(LOG_FREQ, 100).     % Dump every n collections to the log
-define(WIDTH, 30).         % Width of the key part of the formatted logs

%% API
-export([start_link/1]).

-export([format_stats/1]).

-record(state, {bucket, counters=undefined, count=?LOG_FREQ, last_ts}).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-define(NEED_TAP_STREAM_STATS_CODE, 1).
-include("ns_stats.hrl").

start_link(Bucket) ->
    gen_server:start_link(?MODULE, Bucket, []).

init(Bucket) ->
    ns_pubsub:subscribe(ns_tick_event),
    {ok, #state{bucket=Bucket}}.

handle_call(unhandled, unhandled, unhandled) ->
    unhandled.

handle_cast(unhandled, unhandled) ->
    unhandled.

stats_with_tap_stats(Bucket) ->
    {ok, Stats} = ns_memcached:stats(Bucket),
    TapStats = ns_memcached:tap_stats(Bucket),
    {Stats, TapStats}.

handle_info({tick, TS}, #state{bucket=Bucket, counters=Counters, last_ts=LastTS}
            = State) ->
    try stats_with_tap_stats(Bucket) of
        {Stats, TapStats} ->
            TS1 = latest_tick(TS),
            {Entry, NewCounters} = parse_stats(TS1, Stats, TapStats, Counters, LastTS),
            case Counters of % Don't send event with undefined values
                undefined ->
                    ok;
                _ ->
                    gen_event:notify(ns_stats_event, {stats, Bucket, Entry})
            end,
            Count = case State#state.count of
                        ?LOG_FREQ ->
                            case misc:get_env_default(dont_log_stats, false) of
                                false ->
                                    ?log_info("Stats for bucket ~p:~n~s",
                                              [Bucket, format_stats(Stats)]);
                                _ -> ok
                            end,
                            1;
                        C ->
                            C + 1
                    end,
            {noreply, State#state{counters=NewCounters, count=Count,
                                  last_ts=TS1}}
    catch T:E ->
            ?log_error("Exception in stats collector: ~p~n", [{T,E, erlang:get_stacktrace()}]),
            {noreply, State}
    end;
handle_info(_Msg, State) -> % Don't crash on delayed responses to calls
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% Internal functions
format_stats(Stats) ->
    erlang:list_to_binary(
      [[K, lists:duplicate(?WIDTH - byte_size(K), $\s), V, $\n]
       || {K, V} <- lists:sort(Stats)]).


latest_tick(TS) ->
    latest_tick(TS, 0).


latest_tick(TS, NumDropped) ->
    receive
        {tick, TS1} ->
            latest_tick(TS1, NumDropped + 1)
    after 0 ->
            if NumDropped > 0 ->
                    ?log_warning("Dropped ~b ticks", [NumDropped]);
               true ->
                    ok
            end,
            TS
    end.

%% TODO: upgrade this too
translate_stat(bytes) -> % memcached calls it bytes
    mem_used;
translate_stat(ep_num_value_ejects) ->
    evictions;
translate_stat(Stat) ->
    Stat.

sum_stat_values(Dict, [FirstName | RestNames]) ->
    lists:foldl(fun (_Name, null) -> null;
                    (Name, Acc) ->
                        orddict:fetch(Name, Dict) + Acc
                end, orddict:fetch(FirstName, Dict), RestNames).


extract_tap_stream_stats(KVs) ->
    lists:foldl(fun ({K, V}, Acc) -> extact_tap_stat(K, V, Acc) end,
                #tap_stream_stats{}, KVs).

pre_aggregate_single_tap_inner(KVs, Acc, Index) ->
    case lists:keyfind(<<"type">>, 1, KVs) of
        {_, <<"producer">>} ->
            Total = element(4, Acc),
            Mine = element(Index, Acc),
            ThisStats = extract_tap_stream_stats(KVs),
            NewTotal = add_tap_stream_stats(ThisStats, Total),
            NewMine = add_tap_stream_stats(ThisStats, Mine),
            setelement(4, setelement(Index, Acc, NewMine), NewTotal);
        _ -> Acc
    end.

pre_aggregate_single_tap(<<"rebalance_", _/binary>>, KVs, A) ->
    pre_aggregate_single_tap_inner(KVs, A, 1);
pre_aggregate_single_tap(<<"replication_", _/binary>>, KVs, A) ->
    pre_aggregate_single_tap_inner(KVs, A, 2);
pre_aggregate_single_tap(_, KVs, A) ->
    pre_aggregate_single_tap_inner(KVs, A, 3).

pre_aggregate_tap_stats(TapStats) ->
    {RebalanceStats,
     ReplicationStats,
     UserStats,
     TotalStats} = lists:foldl(fun ({Name, KVs}, Acc) ->
                                       pre_aggregate_single_tap(Name, KVs, Acc)
                               end,
                               list_to_tuple(lists:duplicate(4, #tap_stream_stats{})), TapStats),
    lists:append([tap_stream_stats_to_kvlist(<<"ep_tap_rebalance_">>, RebalanceStats),
                  tap_stream_stats_to_kvlist(<<"ep_tap_replica_">>, ReplicationStats),
                  tap_stream_stats_to_kvlist(<<"ep_tap_user_">>, UserStats),
                  tap_stream_stats_to_kvlist(<<"ep_tap_total_">>, TotalStats)]).

parse_stats_raw(TS, Stats, LastCounters, LastTS, KnownGauges, KnownCounters) ->
    GetStat = fun (K, Dict) ->
                      case orddict:find(K, Dict) of
                          {ok, V} -> list_to_integer(binary_to_list(V));
                          %% Some stats don't exist in some bucket types
                          error -> 0
                      end
              end,
    Dict = orddict:from_list([{translate_stat(binary_to_atom(K, latin1)), V}
                              || {K, V} <- Stats]),
    parse_stats_raw2(TS, LastCounters, LastTS, KnownGauges, KnownCounters, GetStat, Dict).

parse_stats_raw2(TS, LastCounters, LastTS, KnownGauges, KnownCounters, GetStat, Dict) ->
    Gauges = [GetStat(K, Dict) || K <- KnownGauges],
    Counters = [GetStat(K, Dict) || K <- KnownCounters],
    Deltas = case LastCounters of
                 undefined ->
                     lists:duplicate(length(KnownCounters), null);
                 _ ->
                     Delta = TS - LastTS,
                     if Delta > 0 ->
                             lists:zipwith(
                               fun (A, B) ->
                                       Res = (A - B) * 1000 div Delta,
                                       if Res < 0 -> 0;
                                          true -> Res
                                       end
                               end, Counters, LastCounters);
                        true ->
                             lists:duplicate(length(KnownCounters),
                                             null)
                     end
             end,
    Values0 = orddict:merge(fun (_, _, _) -> erlang:error(cannot_happen) end,
                            orddict:from_list(lists:zip(KnownGauges, Gauges)),
                            orddict:from_list(lists:zip(KnownCounters, Deltas))),
    {Values0, Counters}.

parse_stats(TS, Stats, TapStats, undefined, LastTS) ->
    parse_stats(TS, Stats, TapStats, {undefined, undefined}, LastTS);
parse_stats(TS, Stats, TapStats, {LastCounters, LastTapCounters}, LastTS) ->
    {StatsValues0, Counters} = parse_stats_raw(TS, Stats, LastCounters, LastTS,
                                               [?STAT_GAUGES], [?STAT_COUNTERS]),
    {TapValues0, TapCounters} = parse_stats_raw(TS, pre_aggregate_tap_stats(TapStats),
                                                LastTapCounters, LastTS,
                                                [?TAP_STAT_GAUGES], [?TAP_STAT_COUNTERS]),
    Values0 = lists:merge(TapValues0, StatsValues0),
    AggregateValues = [{ops, sum_stat_values(Values0, [cmd_get, cmd_set,
                                                       incr_misses, incr_hits,
                                                       decr_misses, decr_hits,
                                                       delete_misses, delete_hits])},
                       {misses, sum_stat_values(Values0, [get_misses, delete_misses,
                                                          incr_misses, decr_misses,
                                                          cas_misses])},
                       %% TODO: consider dead vbuckets or not?
                       {ep_vb_total, sum_stat_values(Values0, [vb_active_num,
                                                               vb_replica_num,
                                                               vb_pending_num])},
                       {ep_ht_memory, sum_stat_values(Values0, [vb_active_ht_memory,
                                                                vb_replica_ht_memory,
                                                                vb_pending_ht_memory])},
                       {disk_writes, sum_stat_values(Values0, [ep_flusher_todo, ep_queue_size])},
                       {vb_total_queue_memory, sum_stat_values(Values0,
                                                               [vb_active_queue_memory,
                                                                vb_replica_queue_memory,
                                                                vb_pending_queue_memory])},
                       {ep_ops_create, sum_stat_values(Values0, [vb_active_ops_create,
                                                                 vb_replica_ops_create,
                                                                 vb_pending_ops_create])},
                       {ep_ops_update, sum_stat_values(Values0, [vb_active_ops_update,
                                                                 vb_replica_ops_update,
                                                                 vb_pending_ops_update])},
                       {vb_total_queue_fill, sum_stat_values(Values0, [vb_active_queue_fill,
                                                                       vb_replica_queue_fill,
                                                                       vb_pending_queue_fill])},
                       {vb_total_queue_drain, sum_stat_values(Values0, [vb_active_queue_drain,
                                                                        vb_replica_queue_drain,
                                                                        vb_pending_queue_drain])},
                       {vb_total_queue_age, sum_stat_values(Values0, [vb_active_queue_age,
                                                                      vb_replica_queue_age,
                                                                      vb_pending_queue_age])}
                       %% {replica_resident_items_tot,
                       %%  orddict:fetch(curr_items_tot,
                       %%                Values0) - orddict:fetch(ep_num_non_resident,
                       %%                                         Values0)},
                       %% {resident_items_tot,
                       %%  orddict:fetch(curr_items,
                       %%                Values0) - orddict:fetch(ep_num_active_non_resident,
                       %% Values0)}
                      ],
    Values = orddict:merge(fun (_K, _V1, _V2) -> erlang:error(cannot_happen) end,
                           Values0, orddict:from_list(AggregateValues)),
    {#stat_entry{timestamp = TS,
                 values = Values},
     {Counters, TapCounters}}.


%% Tests

parse_stats_test() ->
    Now = misc:time_to_epoch_ms_int(now()),
    Input =
        [{<<"conn_yields">>,<<"0">>},
         {<<"threads">>,<<"4">>},
         {<<"rejected_conns">>,<<"0">>},
         {<<"limit_maxbytes">>,<<"67108864">>},
         {<<"bytes_written">>,<<"823281">>},
         {<<"bytes_read">>,<<"37610">>},
         {<<"cas_badval">>,<<"0">>},
         {<<"cas_hits">>,<<"0">>},
         {<<"cas_misses">>,<<"0">>},
         {<<"decr_hits">>,<<"0">>},
         {<<"decr_misses">>,<<"0">>},
         {<<"incr_hits">>,<<"0">>},
         {<<"incr_misses">>,<<"0">>},
         {<<"delete_hits">>,<<"0">>},
         {<<"delete_misses">>,<<"0">>},
         {<<"get_misses">>,<<"0">>},
         {<<"get_hits">>,<<"0">>},
         {<<"auth_errors">>,<<"0">>},
         {<<"auth_cmds">>,<<"0">>},
         {<<"cmd_flush">>,<<"0">>},
         {<<"cmd_set">>,<<"0">>},
         {<<"cmd_get">>,<<"0">>},
         {<<"connection_structures">>,<<"11">>},
         {<<"total_connections">>,<<"11">>},
         {<<"curr_connections">>,<<"11">>},
         {<<"daemon_connections">>,<<"10">>},
         {<<"rusage_system">>,<<"0.067674">>},
         {<<"rusage_user">>,<<"0.149256">>},
         {<<"pointer_size">>,<<"64">>},
         {<<"libevent">>,<<"1.4.14b-stable">>},
         {<<"version">>,<<"1.4.4_209_g7b9e75f">>},
         {<<"time">>,<<"1285969960">>},
         {<<"uptime">>,<<"103">>},
         {<<"pid">>,<<"56368">>},
         {<<"bucket_conns">>,<<"1">>},
         {<<"ep_num_non_resident">>,<<"0">>},
         {<<"ep_pending_ops_max_duration">>,<<"0">>},
         {<<"ep_pending_ops_max">>,<<"0">>},
         {<<"ep_pending_ops_total">>,<<"0">>},
         {<<"ep_pending_ops">>,<<"0">>},
         {<<"ep_io_write_bytes">>,<<"0">>},
         {<<"ep_io_read_bytes">>,<<"0">>},
         {<<"ep_io_num_write">>,<<"0">>},
         {<<"ep_io_num_read">>,<<"0">>},
         {<<"ep_warmup">>,<<"true">>},
         {<<"ep_dbinit">>,<<"0">>},
         {<<"ep_dbname">>,<<"./data/n_0/default">>},
         {<<"ep_tap_keepalive">>,<<"0">>},
         {<<"ep_warmup_time">>,<<"0">>},
         {<<"ep_warmup_oom">>,<<"0">>},
         {<<"ep_warmup_dups">>,<<"0">>},
         {<<"ep_warmed_up">>,<<"0">>},
         {<<"ep_warmup_thread">>,<<"complete">>},
         {<<"ep_num_not_my_vbuckets">>,<<"0">>},
         {<<"ep_num_eject_failures">>,<<"0">>},
         {<<"ep_num_value_ejects">>,<<"5">>},
         {<<"ep_num_expiry_pager_runs">>,<<"0">>},
         {<<"ep_num_pager_runs">>,<<"0">>},
         {<<"ep_bg_fetched">>,<<"0">>},
         {<<"ep_storage_type">>,<<"featured">>},
         {<<"ep_tmp_oom_errors">>,<<"0">>},
         {<<"ep_oom_errors">>,<<"0">>},
         {<<"ep_total_cache_size">>,<<"0">>},
         {<<"ep_mem_high_wat">>,<<"2394685440">>},
         {<<"ep_mem_low_wat">>,<<"1915748352">>},
         {<<"ep_max_data_size">>,<<"3192913920">>},
         {<<"ep_overhead">>,<<"25937168">>},
         {<<"ep_kv_size">>,<<"0">>},
         {<<"mem_used">>,<<"25937168">>},
         {<<"curr_items_tot">>,<<"0">>},
         {<<"curr_items">>,<<"0">>},
         {<<"ep_flush_duration_highwat">>,<<"0">>},
         {<<"ep_flush_duration_total">>,<<"0">>},
         {<<"ep_flush_duration">>,<<"0">>},
         {<<"ep_flush_preempts">>,<<"0">>},
         {<<"ep_vbucket_del_fail">>,<<"0">>},
         {<<"ep_vbucket_del">>,<<"0">>},
         {<<"ep_commit_time_total">>,<<"0">>},
         {<<"ep_commit_time">>,<<"0">>},
         {<<"ep_commit_num">>,<<"0">>},
         {<<"ep_flusher_state">>,<<"running">>},
         {<<"ep_flusher_todo">>,<<"0">>},
         {<<"ep_queue_size">>,<<"0">>},
         {<<"ep_item_flush_expired">>,<<"0">>},
         {<<"ep_expired">>,<<"0">>},
         {<<"ep_item_commit_failed">>,<<"0">>},
         {<<"ep_item_flush_failed">>,<<"0">>},
         {<<"ep_total_persisted">>,<<"0">>},
         {<<"ep_total_del_items">>,<<"0">>},
         {<<"ep_total_new_items">>,<<"0">>},
         {<<"ep_total_enqueued">>,<<"0">>},
         {<<"ep_too_old">>,<<"0">>},
         {<<"ep_too_young">>,<<"0">>},
         {<<"ep_data_age_highwat">>,<<"0">>},
         {<<"ep_data_age">>,<<"0">>},
         {<<"ep_max_txn_size">>,<<"50000">>},
         {<<"ep_queue_age_cap">>,<<"900">>},
         {<<"ep_min_data_age">>,<<"0">>},
         {<<"ep_storage_age_highwat">>,<<"0">>},
         {<<"ep_storage_age">>,<<"0">>},
         {<<"ep_num_not_my_vbuckets">>,<<"0">>},
         {<<"ep_oom_errors">>,<<"0">>},
         {<<"ep_tmp_oom_errors">>,<<"0">>},
         {<<"ep_version">>,<<"1.6.0beta3a_166_g24a1637">>}],

    TestGauges = [curr_connections,
                  curr_items,
                  ep_flusher_todo,
                  ep_queue_size,
                  mem_used],

    TestCounters = [bytes_read,
                    bytes_written,
                    cas_badval,
                    cas_hits,
                    cas_misses,
                    cmd_get,
                    cmd_set,
                    decr_hits,
                    decr_misses,
                    delete_hits,
                    delete_misses,
                    get_hits,
                    get_misses,
                    incr_hits,
                    incr_misses,
                    ep_io_num_read,
                    ep_total_persisted,
                    evictions,
                    ep_num_not_my_vbuckets,
                    ep_oom_errors,
                    ep_tmp_oom_errors],

    ExpectedPropList = [{bytes_read,37610},
                        {bytes_written,823281},
                        {cas_badval,0},
                        {cas_hits,0},
                        {cas_misses,0},
                        {cmd_get,0},
                        {cmd_set,0},
                        {curr_connections,11},
                        {curr_items,0},
                        {decr_hits,0},
                        {decr_misses,0},
                        {delete_hits,0},
                        {delete_misses,0},
                        {ep_flusher_todo,0},
                        {ep_io_num_read,0},
                        {ep_num_not_my_vbuckets,0},
                        {ep_oom_errors,0},
                        {ep_queue_size,0},
                        {ep_tmp_oom_errors,0},
                        {ep_total_persisted,0},
                        {evictions,5},
                        {get_hits,0},
                        {get_misses,0},
                        {incr_hits,0},
                        {incr_misses,0},
                        {mem_used,25937168}],

    {ActualValues,
     ActualCounters} = parse_stats_raw(
                         Now, Input,
                         lists:duplicate(length(TestCounters), 0),
                         Now - 1000,
                         TestGauges, TestCounters),

    ?assertEqual([37610,823281,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,0,0,0],
                 ActualCounters),

    ?assertEqual(lists:keysort(1, ExpectedPropList),
                 lists:keysort(1, ActualValues)).
