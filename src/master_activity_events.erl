%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2012 Couchbase, Inc.
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
-module(master_activity_events).

-include("ns_common.hrl").

-export([start_link_work_server/0,
         start_link_timestamper/0,
         note_ebucketmigrator_start/4,
         note_ebucketmigrator_terminate/2,
         note_deregister_tap_name/3,
         note_vbucket_state_change/4,
         note_bucket_creation/3,
         note_bucket_deletion/1,
         note_rebalance_start/4,
         note_rebalance_end/2,
         note_set_ff_map/3,
         note_set_map/3,
         note_vbucket_mover/6,
         note_move_done/2,
         note_failover/1,
         note_became_master/0,
         note_name_changed/0,
         event_to_jsons/1]).

-export([stream_events/2]).

-define(WORK_SERVER, master_activity_events_srv).

send_event_cast(Arg) ->
    Master = mb_master:master_node(),
    case Master of
        undefined ->
            ?log_debug("sending master_activity_events event to trash can: ~p", [Arg]),
            ok;
        _ ->
            gen_event:notify({master_activity_events_ingress, Master}, Arg)
    end.

submit_cast(Arg) ->
    work_queue:submit_work(?WORK_SERVER, fun () -> send_event_cast(Arg) end).

note_ebucketmigrator_start(Pid, Src, Dst, Options) ->
    submit_cast({ebucketmigrator_start, Pid, Src, Dst, Options}).

note_ebucketmigrator_terminate(Pid, Reason) ->
    submit_cast({ebucketmigrator_terminate, Pid, Reason}).

note_deregister_tap_name(Bucket, Src, Name) ->
    submit_cast({deregister_tap_name, self(), Bucket, Src, Name}).

note_vbucket_state_change(Bucket, Node, VBucketId, NewState) ->
    submit_cast({vbucket_state_change, Bucket, Node, VBucketId, NewState}).

note_bucket_creation(BucketName, BucketType, NewConfig) ->
    submit_cast({create_bucket, BucketName, BucketType, NewConfig}).

note_bucket_deletion(BucketName) ->
    submit_cast({delete_bucket, BucketName}).

note_rebalance_start(Pid, KeepNodes, EjectNodes, FailedNodes) ->
    submit_cast({rebalance_start, Pid, KeepNodes, EjectNodes, FailedNodes}).

note_rebalance_end(Pid, Reason) ->
    submit_cast({rebalance_end, Pid, Reason}).


note_vbucket_mover(Pid, BucketName, Node, VBucketId, OldChain, NewChain) ->
    submit_cast({vbucket_mover_start, Pid, BucketName, Node, VBucketId, OldChain, NewChain}).

note_move_done(BucketName, VBucketId) ->
    submit_cast({vbucket_move_done, BucketName, VBucketId}).

note_failover(Node) ->
    submit_cast({failover, Node}).

note_became_master() ->
    submit_cast({became_master}).

compute_map_diff(NewMap, undefined) ->
    compute_map_diff(NewMap, []);
compute_map_diff(NewMap, []) when NewMap =/= [] ->
    compute_map_diff(NewMap, [[] || _ <- NewMap]);
compute_map_diff(NewMap, OldMap) ->
    true = (NewMap =/= undefined),
    VBucketsCount = erlang:length(NewMap),
    [{I, ChainOld, ChainNew} ||
        {I, ChainOld, ChainNew} <- lists:zip3(lists:seq(0, VBucketsCount-1), OldMap, NewMap),
        ChainOld =/= ChainNew].

note_set_ff_map(BucketName, undefined, _OldMap) ->
    submit_cast({set_ff_map, BucketName, undefined});
note_set_ff_map(BucketName, NewMap, OldMap) ->
    work_queue:submit_work(
      ?WORK_SERVER,
      fun () ->
              send_event_cast({set_ff_map, BucketName, compute_map_diff(NewMap, OldMap)})
      end).

note_set_map(BucketName, NewMap, OldMap) ->
    work_queue:submit_work(
      ?WORK_SERVER,
      fun () ->
              send_event_cast({set_map, BucketName, compute_map_diff(NewMap, OldMap)})
      end).

note_name_changed() ->
    Name = node(),
    submit_cast({name_changed, Name}).

start_link_work_server() ->
    work_queue:start_link(?WORK_SERVER).

start_link_timestamper() ->
    {ok, ns_pubsub:subscribe_link(master_activity_events_ingress, fun timestamper_body/2, [])}.

timestamper_body(Event, _Ignore) ->
    StampedEvent = erlang:list_to_tuple([os:timestamp() | erlang:tuple_to_list(Event)]),
    gen_event:notify(master_activity_events, StampedEvent),
    [].

stream_events(Callback, State) ->
    Ref = make_ref(),
    EofRef = make_ref(),
    Self = self(),
    Fun = fun (Arg, _Ignored) ->
                  Self ! {Ref, Arg},
                  ok
          end,
    LinkPid = ns_pubsub:subscribe_link(master_activity_events, Fun, []),
    try
        case stream_events_history_loop(master_activity_events_keeper:get_history(),
                                        Callback, State, EofRef, undefined) of
            {ok, NewState, LastTS} ->
                CallPredicate =
                    case LastTS of
                        undefined ->
                            fun (_) -> true end;
                        _ ->
                            fun (Event) ->
                                    EventTS = element(1, Event),
                                    timer:now_diff(EventTS, LastTS) > 0
                            end
                    end,
                stream_events_loop(Ref, LinkPid, Callback, NewState, EofRef, CallPredicate);
            {eof, RV} ->
                RV
        end
    after
        ns_pubsub:unsubscribe(LinkPid),
        stream_events_eat_leftover_messages(Ref)
    end.

stream_events_history_loop([], _Callback, State, _EofRef, LastTS) ->
    {ok, State, LastTS};
stream_events_history_loop([Event | HistoryRest], Callback, State, EofRef, _LastTS) ->
    EventTS = element(1, Event),
    case Callback(Event, State, EofRef) of
        {EofRef, RV} ->
            {eof, RV};
        NewState ->
            stream_events_history_loop(HistoryRest, Callback, NewState, EofRef, EventTS)
    end.

stream_events_eat_leftover_messages(Ref) ->
    receive
        {Ref, _} ->
            stream_events_eat_leftover_messages(Ref)
    after 0 ->
            ok
    end.

stream_events_loop(Ref, LinkPid, Callback, State, EofRef, CallPredicate) ->
    receive
        {'EXIT', LinkPid, _Reason} = LinkMsg ->
            ?log_error("Got master_activity_events subscriber link death signal: ~p", [LinkMsg]),
            LinkMsg;
        {Ref, Arg} ->
            case CallPredicate(Arg) of
                true ->
                    ?log_debug("Calling stream_events callback: ~p", [Arg]),
                    case Callback(Arg, State, EofRef) of
                        {EofRef, RV} ->
                            RV;
                        NewState ->
                            stream_events_loop(Ref, LinkPid, Callback, NewState, EofRef, CallPredicate)
                    end;
                false ->
                    stream_events_loop(Ref, LinkPid, Callback, State, EofRef, CallPredicate)
            end
    end.

%% note: spec just marking current dialyzer finding that empty list
%% cannot be passed here, so instead of trying to silence dializer on
%% empty list case (which is not used anyway) I'm doing this to warn
%% any potential future users that empty case needs to be added when
%% needed.
-spec format_simple_plist_as_json(nonempty_list()) -> nonempty_list().
format_simple_plist_as_json(PList) ->
    [PList0H | PList0T] = lists:keysort(1, PList),
    {_, PList1} = lists:foldl(fun ({K, _} = Pair, {PrevK, Acc}) ->
                                 case K =:= PrevK of
                                     true ->
                                         {PrevK, Acc};
                                     false ->
                                         {K, [Pair | Acc]}
                                 end
                         end, {element(1, PList0H), [PList0H]}, PList0T),
    [{Key, if is_list(Value) ->
                   iolist_to_binary(Value);
              is_binary(Value) ->
                   Value;
              true -> iolist_to_binary(io_lib:format("~p", [Value]))
           end}
     || {Key, Value} <- PList1].

format_mcd_pair({Host, Port}) ->
    iolist_to_binary([Host, $:, integer_to_list(Port)]).

node_to_host(undefined, _Config) ->
    <<"">>;
node_to_host(Node, Config) ->
    format_mcd_pair(ns_memcached:host_port(Node, Config)).

event_to_jsons({TS, ebucketmigrator_start, Pid, Src, Dst, Opts}) ->
    {Opts1, MaybeVBuckets} = case lists:keyfind(vbuckets, 1, Opts) of
                                 false ->
                                     {Opts, []};
                                 {vbuckets, VBuckets} ->
                                     {lists:keydelete(vbuckets, 1, Opts),
                                      [{vbuckets, VBuckets}]}
                             end,
    {Opts2, MaybeCheckpoints} = case lists:keyfind(checkpoints, 1, Opts) of
                                    false ->
                                        {Opts1, []};
                                    {checkpoints, Checkpoints} ->
                                        {lists:keydelete(checkpoints, 1, Opts1),
                                         [{checkpoints, {struct, Checkpoints}}]}
                                end,
    [MaybeVBuckets ++ MaybeCheckpoints ++
         format_simple_plist_as_json([{type, ebucketmigratorStart},
                                      {ts, misc:time_to_epoch_float(TS)},
                                      {node, erlang:node(Pid)},
                                      {pid, Pid},
                                      {src, format_mcd_pair(Src)},
                                      {dst, format_mcd_pair(Dst)} | Opts2])];
event_to_jsons({TS, ebucketmigrator_terminate, Pid, Reason}) ->
    [format_simple_plist_as_json([{type, ebucketmigratorTerminate},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {pid, Pid},
                                  {reason, Reason}])];
event_to_jsons({TS, deregister_tap_name, Pid, Bucket, Src, Name}) ->
    [format_simple_plist_as_json([{type, deregisterTapName},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {bucket, Bucket},
                                  {pid, Pid},
                                  {pidNode, erlang:node(Pid)},
                                  {host, format_mcd_pair(Src)},
                                  {name, Name}])];
event_to_jsons({TS, vbucket_state_change, Bucket, Node, VBucketId, NewState}) ->
    Host = ns_memcached:host_port(Node),
    [format_simple_plist_as_json([{type, vbucketStateChange},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {bucket, Bucket},
                                  {host, format_mcd_pair(Host)},
                                  {vbucket, VBucketId},
                                  {state, NewState}])];

event_to_jsons({TS, set_ff_map, BucketName, undefined}) ->
    [format_simple_plist_as_json([{type, resetFastForwardMap},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {bucket, BucketName}])];

event_to_jsons({TS, SetMap, BucketName, Diff}) when SetMap =:= set_map orelse SetMap =:= set_ff_map ->
    Config = ns_config:get(),
    [begin
         Type = case SetMap of
                    set_map -> updateMap;
                    set_ff_map -> updateFastForwardMap
                end,
         format_simple_plist_as_json([{type, Type},
                                      {ts, misc:time_to_epoch_float(TS)},
                                      {bucket, BucketName},
                                      {vbucket, I}])
             ++ [{chainBefore, [node_to_host(N, Config) || N <- OldChain]},
                 {chainAfter, [node_to_host(N, Config) || N <- NewChain]}]
     end || {I, OldChain, NewChain} <- Diff];

event_to_jsons({TS, rebalance_start, Pid, KeepNodes, EjectNodes, FailedNodes}) ->
    Config = ns_config:get(),
    [format_simple_plist_as_json([{type, rebalanceStart},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {pid, Pid}])
     ++ [{keepNodes, [node_to_host(N, Config) || N <- KeepNodes]},
         {ejectNodes, [node_to_host(N, Config) || N <- EjectNodes]},
         {failedNodes, [node_to_host(N, Config) || N <- FailedNodes]}]];
event_to_jsons({TS, rebalance_end, Pid, Reason}) ->
    [format_simple_plist_as_json([{type, rebalanceEnd},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {pid, Pid},
                                  {reason, iolist_to_binary(io_lib:format("~p", [Reason]))}])];

event_to_jsons({TS, vbucket_mover_start, Pid, BucketName, Node, VBucketId, OldChain, NewChain}) ->
    Config = ns_config:get(),
    [format_simple_plist_as_json([{type, vbucketMoverStart},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {pid, Pid},
                                  {bucket, BucketName},
                                  {node, Node},
                                  {vbucket, VBucketId}])
     ++ [{oldChain, [node_to_host(N, Config) || N <- OldChain]},
         {newChain, [node_to_host(N, Config) || N <- NewChain]}]];

event_to_jsons({TS, vbucket_move_done, BucketName, VBucketId}) ->
    [format_simple_plist_as_json([{type, vbucketMoveDone},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {bucket, BucketName},
                                  {vbucket, VBucketId}])];

event_to_jsons({TS, failover, Node}) ->
    [format_simple_plist_as_json([{type, failover},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {host, node_to_host(Node, ns_config:get())}])];

event_to_jsons({TS, became_master}) ->
    [format_simple_plist_as_json([{type, becameMaster},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {node, node()},
                                  {host, node_to_host(node(), ns_config:get())}])];

event_to_jsons({TS, create_bucket, BucketName, BucketType, NewConfig}) ->
    [format_simple_plist_as_json([{type, createBucket},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {bucket, BucketName},
                                  {bucketType, BucketType}])
                                 ++ [{params, {struct, format_simple_plist_as_json(NewConfig)}}]];

event_to_jsons({TS, delete_bucket, BucketName}) ->
    [format_simple_plist_as_json([{type, deleteBucket},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {bucket, BucketName}])];

event_to_jsons({TS, name_changed, NewName}) ->
    [format_simple_plist_as_json([{type, nameChanged},
                                  {ts, misc:time_to_epoch_float(TS)},
                                  {node, NewName},
                                  {host, node_to_host(NewName, ns_config:get())}])];

event_to_jsons(Event) ->
    ?log_warning("Got unknown kind of event: ~p", [Event]),
    [].
