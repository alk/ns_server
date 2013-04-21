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

-define(DOCS_LEFT_REFRESH_INTERVAL, 5000).

-record(replica_building_stats, {node :: node(),
                                 docs_total :: non_neg_integer(),
                                 docs_left :: non_neg_integer(),
                                 tap_name = <<"">> :: binary()}).

-record(move_state, {vbucket :: vbucket_id(),
                     before_chain :: [node()],
                     after_chain :: [node()],
                     stats :: [#replica_building_stats{}]}).

-record(state, {bucket :: bucket_name() | undefined,
                done_moves :: [#move_state{}],
                current_moves :: [#move_state{}],
                pending_moves :: [#move_state{}]
               }).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

is_interesting_master_event({_, set_ff_map, _BucketName, _Diff}) -> fun handle_set_ff_map/2;
is_interesting_master_event({_, vbucket_move_start, _Pid, _BucketName, _Node, _VBucketId, _, _}) -> fun handle_vbucket_move_start/2;
is_interesting_master_event({_, vbucket_move_done, _BucketName, _VBucketId}) -> fun handle_vbucket_move_done/2;
is_interesting_master_event({_, tap_estimate, _, _, _, _}) -> fun handle_tap_estimate/2;
is_interesting_master_event(_) -> undefined.


init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(master_activity_events,
                             fun (Event, _Ignored) ->
                                     case is_interesting_master_event(Event) of
                                         undefined ->
                                             [];
                                         Fun ->
                                             gen_server:cast(Self, {note, Fun, Event})
                                     end
                             end, []),

    {ok, _} = timer2:send_interval(5000, log_state),

    proc_lib:spawn_link(erlang, apply, [fun docs_left_updater_init/1, [Self]]),

    {ok, #state{bucket = undefined,
                done_moves  = [],
                current_moves = [],
                pending_moves = []}}.

handle_call(get, _From, State) ->
    {reply, State, State};
handle_call(Req, From, State) ->
    ?log_error("Got unknown request: ~p from ~p", [Req, From]),
    {reply, unknown_request, State}.

handle_cast({note, Fun, Ev}, State) ->
    {noreply, NewState} = Fun(Ev, State),
    ?log_debug("Handled ~p:~n~p~n =>~n~p", [Ev, State, NewState]),
    {noreply, NewState};

handle_cast({update_stats, VBucket, NodeToDocsLeft}, State) ->
    ?log_debug("Got update_stats: ~p, ~p~n~p", [VBucket, NodeToDocsLeft, State]),
    {noreply, update_move(
                State, VBucket,
                fun (Move) ->
                        NewStats =
                            [case lists:keyfind(Stat#replica_building_stats.node, 1, NodeToDocsLeft) of
                                 {_, NewLeft} ->
                                     Stat#replica_building_stats{docs_left = NewLeft};
                                 false ->
                           Stat
                             end || Stat <- Move#move_state.stats],
                        Move#move_state{stats = NewStats}
                end)};

handle_cast(Req, _State) ->
    ?log_error("Got unknown cast: ~p", [Req]),
    erlang:error({unknown_cast, Req}).

initiate_bucket_rebalance(BucketName) ->
    {ok, BucketConfig} = ns_bucket:get_bucket(BucketName),
    Map = proplists:get_value(map, BucketConfig),
    FFMap = case proplists:get_value(fastForwardMap, BucketConfig) of
                undefined ->
                    %% yes this is possible if rebalance completes
                    %% faster than we can start observing it's
                    %% progress
                    Map;
                FFMap0 ->
                    FFMap0
            end,
    VBCount = length(Map),
    Diff = [Triple
            || {_, [MasterNode|_] = ChainBefore, ChainAfter} = Triple <- lists:zip3(lists:seq(0, VBCount-1),
                                                                                    Map,
                                                                                    FFMap),
               MasterNode =/= undefined,
               ChainBefore =/= ChainAfter],
    BuildDestinations0 = [{MasterNode, VB} || {VB, [MasterNode|_], _ChainAfter} <- Diff],
    BuildDestinations1 = [{N, VB} || {VB, _, ChainAfter} <- Diff,
                                     N <- ChainAfter],

    BuildDestinations =
        %% the following groups vbuckets to per node. [{a, 1}, {a, 2}, {b, 3}] => [{a, [1,2]}, {b, [3]}]
        lists:foldr(
          fun ({N, VB}, Acc) ->
                  case Acc of
                      [{N, AccVBs} | Rest] ->
                          [{N, [VB | AccVBs]} | Rest];
                      _ ->
                          [{N, [VB]} | Acc]
                  end
          end, [], lists:merge(lists:sort(BuildDestinations0), lists:sort(BuildDestinations1))),

    ?log_debug("BuildDestinations:~n~p", [BuildDestinations]),
    SomeEstimates0 = misc:parallel_map(
                       fun ({Node, VBs}) ->
                               {ok, Estimates} = janitor_agent:get_mass_tap_docs_estimate(BucketName, Node, VBs),
                               [{{Node, VB}, VBEstimate} || {VB, VBEstimate} <- lists:zip(VBs, Estimates)]
                       end, BuildDestinations, infinity),


    SomeEstimates = lists:append(SomeEstimates0),

    ?log_debug("SomeEstimates:~n~p", [SomeEstimates]),

    Moves =
        [begin
             {_, MasterEstimate} = lists:keyfind({MasterNode, VB}, 1, SomeEstimates),
             RBStats =
                 [begin
                      {_, ReplicaEstimate} = lists:keyfind({Replica, VB}, 1, SomeEstimates),
                      Estimate = case ReplicaEstimate =< MasterEstimate of
                                     true ->
                                         %% in this case we assume no
                                         %% backfill is required
                                         MasterEstimate - ReplicaEstimate;
                                     _ ->
                                         MasterEstimate
                                 end,
                      #replica_building_stats{node = Replica,
                                              docs_total = Estimate,
                                              docs_left = Estimate}
                  end || Replica <- ChainAfter,
                         Replica =/= undefined,
                         Replica =/= MasterNode],
             #move_state{vbucket = VB,
                         before_chain = ChainBefore,
                         after_chain = ChainAfter,
                         stats = RBStats}
         end || {VB, [MasterNode|_] = ChainBefore, ChainAfter} <- Diff],

    #state{bucket = BucketName,
           done_moves = [],
           current_moves = [],
           pending_moves = Moves}.

handle_set_ff_map({_, set_ff_map, BucketName, _Diff}, _State) ->
    {noreply, initiate_bucket_rebalance(BucketName)}.

handle_vbucket_move_start({_, vbucket_move_start, _Pid, _BucketName, _Node, VBucketId, _, _} = Ev, State) ->
    case ensure_not_pending(State, VBucketId) of
        State ->
            ?log_error("Weird vbucket move start for move not in pending moves: ~p", [Ev]),
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end.

handle_tap_estimate({_, tap_estimate, {_Type, _BucketName, VBucket, _Src, Dst}, Estimate, _Pid, TapName} = Ev, State) ->
    ?log_debug("seeing tap_estimate: ~p", [Ev]),
    State2 = ensure_not_pending(State, VBucket),
    State3 = update_tap_estimate(
               State2, VBucket, Dst,
               fun (Stat) ->
                       Stat#replica_building_stats{docs_left = Estimate,
                                                   docs_total = Estimate,
                                                   tap_name = TapName}
               end),
    ?log_debug("State after handling tap_estimate:~n~p", [State3]),
    {noreply, State3}.

handle_vbucket_move_done({_, vbucket_move_done, _BucketName, VBucket} = Ev, State) ->
    case ensure_not_current(State, VBucket) of
        State ->
            ?log_error("Weird vbucket_move_done for move not in current_moves: ~p", [Ev]),
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end.

move_the_move(State, VBucketId, From, To) ->
    case lists:keytake(VBucketId, #move_state.vbucket, erlang:element(From, State)) of
        false ->
            ?log_debug("Failing move_the_move(~p, ~p, ~p, ~p) (~p)", [State, VBucketId, From, To, erlang:element(From, State)]),
            State;
        {value, Move, RestFrom} ->
            OldTo = erlang:element(To, State),
            State1 = erlang:setelement(To, State, [Move | OldTo]),
            erlang:setelement(From, State1, RestFrom)
    end.

ensure_not_pending(State, VBucketId) ->
    move_the_move(State, VBucketId, #state.pending_moves, #state.current_moves).

ensure_not_current(State, VBucketId) ->
    move_the_move(State, VBucketId, #state.current_moves, #state.done_moves).

update_move(#state{current_moves = Moves} = State, VBucket, Fun) ->
    NewCurrent =
        [case Move#move_state.vbucket =:= VBucket of
             false ->
                 Move;
             _ ->
                 Fun(Move)
         end || Move <- Moves],
    State#state{current_moves = NewCurrent}.

update_tap_estimate(State, VBucket, Dst, Fun) ->
    update_move(State, VBucket,
                fun (Move) ->
                        update_tap_estimate_in_move(Move, Dst, Fun)
                end).

update_tap_estimate_in_move(#move_state{stats = RStats} = Move, Dst, Fun) ->
    Move#move_state{
      stats = [case Stat#replica_building_stats.node =:= Dst of
                   false ->
                       Stat;
                   _ ->
                       Fun(Stat)
               end || Stat <- RStats]}.

handle_info(log_state, State) ->
    case State#state.bucket of
        undefined ->
            ok;
        _ ->
            ?log_info("rebalance observer state:~n~p", [State])
    end,
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

docs_left_updater_init(Parent) ->
    {ok, _} = timer2:send_interval(?DOCS_LEFT_REFRESH_INTERVAL, refresh),
    docs_left_updater_loop(Parent).

docs_left_updater_loop(Parent) ->
    #state{current_moves = CurrentMoves,
           bucket = BucketName} = gen_server:call(Parent, get, infinity),
    case BucketName of
        undefined ->
            ok;
        _ ->
            ?log_debug("Starting docs_left_updater_loop:~p~n~p", [BucketName, CurrentMoves])
    end,
    [update_docs_left_for_move(Parent, BucketName, M) || M <- CurrentMoves],
    receive
        refresh ->
            _Lost = misc:flush(refresh),
            docs_left_updater_loop(Parent)
    end.

update_docs_left_for_move(Parent, BucketName,
                          #move_state{vbucket = VBucket,
                                      before_chain = [MasterNode|_],
                                      stats = RStats}) ->
    TapNames = [S#replica_building_stats.tap_name || S <- RStats],
    ?log_debug("TapNames: ~p", [TapNames]),
    %% TODO: rework not my vbucket here
    try janitor_agent:get_tap_docs_estimate_many_taps(BucketName, MasterNode, VBucket, TapNames) of
        NewLefts ->
            Stuff = [case OkE of
                         {ok, E} -> {Stat#replica_building_stats.node, E}
                     end || {OkE, Stat} <- lists:zip(NewLefts, RStats)],
            gen_server:cast(Parent, {update_stats, VBucket, Stuff})
    catch error:{janitor_agent_servant_died, _} ->
            ?log_debug("Apparently move of ~p is already done", [VBucket]),
            ok
    end.
