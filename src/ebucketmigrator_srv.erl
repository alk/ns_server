%% @author Northscale <info@northscale.com>
%% @copyright 2010 NorthScale, Inc.
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
-module(ebucketmigrator_srv).

-behaviour(gen_server).

-include("ns_common.hrl").

-define(SERVER, ?MODULE).
-define(CONNECT_TIMEOUT, 5000).        % Milliseconds
-define(UPSTREAM_TIMEOUT, 600000000).   % Microseconds because we use timer:now_diff
-define(TIMEOUT_CHECK_INTERVAL, 15000). % Milliseconds
-define(TERMINATE_TIMEOUT, 30000).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {bad_vbucket_count = 0 :: non_neg_integer(),
                upstream :: port(),
                downstream :: port(),
                upstream_sender :: pid(),
                upbuf = <<>> :: binary(),
                downbuf = <<>> :: binary(),
                vbuckets,
                last_sent_seqno = -1 :: integer(),
                takeover :: boolean(),
                takeover_done :: boolean(),
                takeover_msgs_seen = 0 :: non_neg_integer(),
                last_seen
               }).

%% external API
-export([start_link/3, start_link/4, leech_life/1, start_link_with_substance/4]).

-include("mc_constants.hrl").
-include("mc_entry.hrl").


%%
%% gen_server callback implementation
%%

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_nothing() ->
    receive
        {'EXIT', _From, Reason} = ExitMsg ->
            ?log_debug("Got exit signal: ~p", [ExitMsg]),
            exit(Reason);
        X ->
            ?log_debug("leeched ebucketmigrator got message:~n~p", [X]),
            do_nothing()
    end.

-spec start_substance(pid(), #state{}) -> no_return().
start_substance(Pid, #state{downstream = Downstream} = State) ->
    erlang:link(Pid),
    gen_tcp:controlling_process(Downstream, self()),
    erlang:link(Downstream),
    proc_lib:init_ack({ok, self()}),
    substance_loop(State, Pid).

substance_loop(State, Pid) ->
    receive
        {'$gen_call', From, Msg} ->
            check_downstream = Msg,
            RV = confirm_sent_messages(State),
            gen_server:reply(From, {RV, State}),
            erlang:unlink(Pid),
            exit(normal);
        {'EXIT', _From, Reason} = ExitMsg ->
            ?log_debug("Substance is dying: ~p", [ExitMsg]),
            exit(Reason);
        X ->
            exit({unknown_message, X})
    end.

handle_call({leech_life, Pid}, From, #state{downstream = Downstream} = State) ->
    terminate(leeched, State),
    State2 = State#state{last_sent_seqno = State#state.last_sent_seqno+1},
    {ok, Substance} = proc_lib:start(erlang, apply, [fun start_substance/2, [Pid, State2]]),
    gen_server:reply(From, Substance),
    erlang:unlink(Downstream),
    do_nothing();

handle_call(_Req, _From, State) ->
    {reply, unhandled, State}.


handle_cast(Msg, State) ->
    ?rebalance_error("Unhandled cast: ~p", [Msg]),
    {noreply, State}.


handle_info(retry_not_ready_vbuckets, _State) ->
    exit_retry_not_ready_vbuckets();
handle_info({tcp, Socket, Data}, #state{downstream=Downstream,
                                        upstream=Upstream} = State) ->
    %% Set up the socket to receive another message
    ok = inet:setopts(Socket, [{active, once}]),
    State1 = case Socket of
                 Downstream ->
                     process_data(Data, #state.downbuf,
                                  fun process_downstream/2, State);
                 Upstream ->
                     process_data(Data, #state.upbuf,
                                  fun process_upstream/2,
                                  State#state{last_seen=now()})
    end,
    {noreply, State1};
handle_info({tcp_closed, Socket}, #state{upstream=Socket} = State) ->
    case State#state.takeover of
        true ->
            N = sets:size(State#state.vbuckets),
            case State#state.takeover_msgs_seen of
                N ->
                    {stop, normal, State#state{takeover_done = true}};
                Msgs ->
                    {stop, {wrong_number_takeovers, Msgs, N}, State}
            end;
        false ->
            {stop, normal, State}
    end;
handle_info({tcp_closed, Socket}, #state{downstream=Socket} = State) ->
    {stop, downstream_closed, State};
handle_info(check_for_timeout, State) ->
    case timer:now_diff(now(), State#state.last_seen) > ?UPSTREAM_TIMEOUT of
        true ->
            {stop, timeout, State};
        false ->
            {noreply, State}
    end;
handle_info({'EXIT', Pid, Reason}, #state{upstream_sender = SenderPid} = State) when Pid =:= SenderPid ->
    ?log_error("killing myself due to unexpected upstream sender exit with reason: ~p", [Reason]),
    {stop, {unexpected_upstream_sender_exit, Reason}, State};
handle_info(Msg, State) ->
    ?rebalance_info("handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.


init({Src, Dst, Opts}) ->
    Username = proplists:get_value(username, Opts),
    Password = proplists:get_value(password, Opts, ""),
    Bucket = proplists:get_value(bucket, Opts),
    VBuckets = proplists:get_value(vbuckets, Opts, [0]),
    TakeOver = proplists:get_bool(takeover, Opts),
    TapSuffix = proplists:get_value(suffix, Opts),
    Name = case TakeOver of
               true -> "rebalance_" ++ TapSuffix;
               _ -> "replication_" ++ TapSuffix
           end,
    Substance = proplists:get_value(substance, Opts),
    case Substance of
        undefined ->
            proc_lib:init_ack({ok, self()});
        _ ->
            ok
    end,
    Downstream = connect(Dst, Username, Password, Bucket),
    SafeToReuse = case Substance of
                      undefined ->
                          false;
                      _ ->
                          case (catch gen_server:call(Substance, check_downstream)) of
                              {ok, _OldState} ->
                                  true;
                              Crap ->
                                  ?log_debug("check_downstream failed: ~p", [Crap]),
                                  false
                          end
                  end,
    case Substance of
        undefined ->
            ok;
        _ ->
            proc_lib:init_ack({ok, self()})
    end,
    %% Set all vbuckets to the replica state on the destination node.
    lists:foreach(
      fun (VBucket) ->
              ok = mc_client_binary:set_vbucket(Downstream, VBucket, replica)
      end, VBuckets),
    Upstream = connect(Src, Username, Password, Bucket),
    {ok, CheckpointIdsDict} = mc_client_binary:get_open_checkpoint_ids(Upstream),
    ?rebalance_debug("CheckpointIdsDict:~n~p~n", [CheckpointIdsDict]),
    ReadyVBuckets = lists:filter(
                      fun (Vb) ->
                              case dict:find(Vb, CheckpointIdsDict) of
                                  {ok, X} when is_integer(X) andalso X > 0 -> true;
                                  _ -> false
                              end
                      end, VBuckets),
    if
        ReadyVBuckets =/= VBuckets ->
            false = TakeOver,
            ?rebalance_info("Some vbuckets were not yet ready to replicate from:~n~p~n",
                            [VBuckets -- ReadyVBuckets]),
            erlang:send_after(30000, self(), retry_not_ready_vbuckets),
            if ReadyVBuckets =:= [] ->
                    gen_tcp:close(Upstream),
                    gen_tcp:close(Downstream),
                    receive retry_not_ready_vbuckets -> ok end,
                    exit_retry_not_ready_vbuckets();
               true -> ok
            end;
        true -> ok
    end,
    Checkpoints = lists:map(fun ({V, {ok, C}}) -> {V, C};
                                ({V, _})       -> {V, 0}
                            end,
                            [{Vb, mc_client_binary:get_last_closed_checkpoint(Downstream, Vb)} || Vb <- ReadyVBuckets]),
    Args = [{vbuckets, ReadyVBuckets},
            {checkpoints, Checkpoints},
            {name, Name},
            {takeover, TakeOver}],
    ?rebalance_info("Starting tap stream:~n~p~n", [Args]),
    case SafeToReuse of
        false ->
            mc_client_binary:deregister_tap_client(Upstream, iolist_to_binary(Name));
        true ->
            ok
    end,
    {ok, quiet} = mc_client_binary:tap_connect(Upstream, Args),
    ok = inet:setopts(Upstream, [{active, once}]),
    ok = inet:setopts(Downstream, [{active, once}]),

    Timeout = proplists:get_value(timeout, Opts, ?TIMEOUT_CHECK_INTERVAL),
    {ok, _TRef} = timer:send_interval(Timeout, check_for_timeout),

    UpstreamSender = spawn_link(erlang, apply, [fun upstream_sender_loop/1, [Upstream]]),
    ?rebalance_debug("upstream_sender pid: ~p", [UpstreamSender]),

    State = #state{
      upstream=Upstream,
      downstream=Downstream,
      upstream_sender = UpstreamSender,
      vbuckets=sets:from_list(ReadyVBuckets),
      last_seen=now(),
      takeover=TakeOver,
      takeover_done=false
     },
    erlang:process_flag(trap_exit, true),
    gen_server:enter_loop(?MODULE, [], State).

upstream_sender_loop(Upstream) ->
    receive
        Data ->
            ok = gen_tcp:send(Upstream, Data)
    end,
    upstream_sender_loop(Upstream).

exit_retry_not_ready_vbuckets() ->
    ?log_info("dying to check if some previously not yet ready vbuckets are ready to replicate from"),
    exit(normal).

terminate(_Reason, #state{upstream_sender=UpstreamSender} = State) ->
    timer:kill_after(?TERMINATE_TIMEOUT),
    gen_tcp:close(State#state.upstream),
    (catch exit(UpstreamSender, kill)),
    case State#state.takeover_done of
        true ->
            ?rebalance_info("Skipping close ack for successfull takover~n", []),
            ok;
        _ ->
            confirm_sent_messages(State)
    end.

read_tap_message(Sock) ->
    case gen_tcp:recv(Sock, ?HEADER_LEN) of
        {ok, <<_Magic:8, _Opcode:8, _KeyLen:16, _ExtLen:8, _DataType: 8,
               _VBucket:16, BodyLen:32, _Opaque:32, _CAS:64>> = Packet} ->
            case BodyLen of
                0 ->
                    {ok, Packet};
                _ ->
                    case gen_tcp:recv(Sock, BodyLen) of
                        {ok, Extra} ->
                            {ok, <<Packet/binary, Extra/binary>>};
                        X1 ->
                            X1
                    end
            end;
        X2 ->
            X2
    end.

do_confirm_sent_messages(Sock, Seqno) ->
    case read_tap_message(Sock) of
        {ok, Packet} ->
            <<_Magic:8, _Opcode:8, _KeyLen:16, _ExtLen:8, _DataType: 8,
              _VBucket:16, _BodyLen:32, Opaque:32, _CAS:64, _Rest/binary>> = Packet,
            case Opaque of
                Seqno ->
                    ?rebalance_info("Got close ack!~n", []),
                    ok;
                _ ->
                    do_confirm_sent_messages(Sock, Seqno)
            end;
        {error, _} = Crap ->
            ?rebalance_info("Got error while trying to read close ack:~p~n",
                            [Crap]),
            Crap
    end.

confirm_sent_messages(State) ->
    Seqno = State#state.last_sent_seqno + 1,
    Sock = State#state.downstream,
    inet:setopts(Sock, [{active, false}, {nodelay, true}]),
    Msg = mc_binary:encode(req, #mc_header{opcode = ?TAP_OPAQUE, opaque = Seqno},
                           #mc_entry{data = <<4:16, ?TAP_FLAG_ACK:16, 1:8, 0:8, 0:8, 0:8, ?TAP_OPAQUE_CLOSE_TAP_STREAM:32>>}),
    case gen_tcp:send(Sock, Msg) of
        ok ->
            do_confirm_sent_messages(Sock, Seqno);
        {error, closed} ->
            ok;
        X ->
            ?rebalance_error("Got error while trying to send close confirmation: ~p~n", [X]),
            X
    end.

%%
%% API
%%

start_link(Src, Dst, Opts) ->
    start_link(node(), Src, Dst, Opts).

%% Starts ebucketmigrator on the `Node'.
start_link(Node, Src, Dst, Opts) ->
    misc:start_link(Node, ?MODULE, init, [{Src, Dst, Opts}]).

start_link_with_substance(Substance, Src, Dst, Opts) ->
    start_link(Src, Dst, [{substance, Substance} | Opts]).

leech_life(Pid) ->
    gen_server:call(Pid, {leech_life, self()}).


%%
%% Internal functions
%%

connect({Host, Port}, Username, Password, Bucket) ->
    {ok, Sock} = gen_tcp:connect(Host, Port,
                                 [binary, {packet, raw}, {active, false},
                                  {recbuf, 10*1024*1024},
                                  {sndbuf, 10*1024*1024}],
                                 ?CONNECT_TIMEOUT),
    case Username of
        undefined ->
            ok;
        _ ->
            ok = mc_client_binary:auth(Sock, {<<"PLAIN">>,
                                              {list_to_binary(Username),
                                               list_to_binary(Password)}})
    end,
    case Bucket of
        undefined ->
            ok;
        _ ->
            ok = mc_client_binary:select_bucket(Sock, Bucket)
    end,
    Sock.


%% @doc Chop up a buffer into packets, calling the callback with each packet.
-spec process_data(binary(), fun((binary(), #state{}) -> {binary(), #state{}}),
                                #state{}) -> {binary(), #state{}}.
process_data(<<_Magic:8, Opcode:8, _KeyLen:16, _ExtLen:8, _DataType:8,
               _VBucket:16, BodyLen:32, _Opaque:32, _CAS:64, _Rest/binary>>
                 = Buffer, CB, State)
  when byte_size(Buffer) >= BodyLen + ?HEADER_LEN ->
    %% We have a complete command
    {Packet, NewBuffer} = split_binary(Buffer, BodyLen + ?HEADER_LEN),
    State1 =
        case Opcode of
            ?NOOP ->
                %% These aren't normal TAP packets; eating them here
                %% makes everything else easier.
                State;
            _ ->
                CB(Packet, State)
        end,
    process_data(NewBuffer, CB, State1);
process_data(Buffer, _CB, State) ->
    %% Incomplete
    {Buffer, State}.


%% @doc Append Data to the appropriate buffer, calling the given
%% callback for each packet.
-spec process_data(binary(), non_neg_integer(),
                   fun((binary(), #state{}) -> #state{}), #state{}) -> #state{}.
process_data(Data, Elem, CB, State) ->
    Buffer = element(Elem, State),
    {NewBuf, NewState} = process_data(<<Buffer/binary, Data/binary>>, CB, State),
    setelement(Elem, NewState, NewBuf).


%% @doc Process a packet from the downstream server.
-spec process_downstream(<<_:8,_:_*8>>, #state{}) ->
                                #state{}.
process_downstream(<<?RES_MAGIC:8, _/binary>> = Packet,
                   State) ->
    State#state.upstream_sender ! Packet,
    State.


%% @doc Process a packet from the upstream server.
-spec process_upstream(<<_:64,_:_*8>>, #state{}) ->
                              #state{}.
process_upstream(<<?REQ_MAGIC:8, Opcode:8, _KeyLen:16, _ExtLen:8, _DataType:8,
                   VBucket:16, _BodyLen:32, Opaque:32, _CAS:64, _EnginePriv:16,
                   _Flags:16, _TTL:8, _Res1:8, _Res2:8, _Res3:8, Rest/binary>> =
                     Packet,
                 #state{downstream=Downstream, vbuckets=VBuckets} = State) ->
    case Opcode of
        ?TAP_OPAQUE ->
            ok = gen_tcp:send(Downstream, Packet),
            case Rest of
                <<?TAP_OPAQUE_INITIAL_VBUCKET_STREAM:32>> ->
                    ?rebalance_info("Initial stream for vbucket ~p",
                                    [VBucket]);
                _ ->
                    ok
            end,
            State;
        _ ->
            State1 =
                case Opcode of
                    ?TAP_VBUCKET ->
                        case Rest of
                            <<?VB_STATE_ACTIVE:32>> ->
                                true = State#state.takeover,
                                %% VBucket has been transferred, count it
                                State#state{takeover_msgs_seen =
                                                State#state.takeover_msgs_seen
                                            + 1};
                            <<_:32>> -> % Make sure it's still a 32 bit value
                                State
                        end;
                    _ ->
                        State
                end,
            case sets:is_element(VBucket, VBuckets) of
                true ->
                    ok = gen_tcp:send(Downstream, Packet),
                    State1#state{last_sent_seqno = Opaque};
                false ->
                    %% Filter it out and count it
                    State1#state{bad_vbucket_count =
                                     State1#state.bad_vbucket_count + 1}
            end
    end.
