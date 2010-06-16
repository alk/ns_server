% Copyright (c) 2010, NorthScale, Inc.
% All rights reserved.

-module(ns_node_disco).

-behaviour(gen_server).
-behavior(ns_log_categorizing).

-define(PING_FREQ, 60000).
-define(NODE_CHANGE_DELAY, 5000).
-define(SYNC_TIMEOUT, 5000).

-define(COOKIE_INHERITED, 1).
-define(COOKIE_SYNCHRONIZED, 2).
-define(COOKIE_GEN, 3).
-define(NODE_UP, 4).
-define(NODE_DOWN, 5).

%% API
-export([start_link/0,
         nodes_wanted/0,
         nodes_wanted_updated/0,
         nodes_actual/0,
         random_node/0,
         nodes_actual_proper/0,
         nodes_actual_other/0,
         cookie_init/0, cookie_gen/0,
         cookie_get/0, cookie_set/1, cookie_sync/0]).

-export([ns_log_cat/1, ns_log_code_string/1]).

%% gen_server

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {nodes, timer}).

-include_lib("eunit/include/eunit.hrl").

% Node Discovery and monitoring.
%
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Returns all nodes that we see.
%
% TODO: Track flapping nodes and attenuate, or figure out
%       how to configure OTP to do it, if not already.

nodes_actual() ->
    lists:sort(nodes([this, visible])).

nodes_actual_proper() ->
    gen_server:call(?MODULE, nodes_actual_proper).

random_node() ->
    WorkingNodes = lists:filter(fun(N) ->
                                        net_adm:ping(N) == pong
                                end,
                                nodes_wanted() -- [node()]),

    case WorkingNodes of
        [] -> exit(nonode);
        [N|_] -> N
    end.

% Returns nodes_actual_proper(), but with self node() filtered out.
% This is not the same as nodes([visible]), because this function may
% return a subset of nodes([visible]), similar to nodes_actual_proper().

nodes_actual_other() ->
    lists:subtract(nodes_actual_proper(), [node()]).

nodes_wanted() ->
    gen_server:call(?MODULE, nodes_wanted).

% API's used as callbacks that are invoked when ns_config
% keys have changed.

nodes_wanted_updated() ->
    gen_server:cast(?MODULE, nodes_wanted_updated).

%% gen_server implementation

init([]) ->
    error_logger:info_msg("Initting ns_node_disco with ~p~n", [nodes()]),
    % Proactively run one round of reconfiguration update.
    ok = misc:wait_for_process(do_nodes_wanted_updated(do_nodes_wanted()),
                              ?SYNC_TIMEOUT),
    % Register for nodeup/down messages as handle_info callbacks.
    ok = net_kernel:monitor_nodes(true),
    {ok, Timer} = timer:send_interval(?PING_FREQ, ping_all),
    % Track the last list of actual ndoes.
    io:format("ns_node_disco:init completion~n"),
    {ok, #state{nodes = false, timer = Timer}}.

terminate(_Reason, State) ->
    timer:cancel(State#state.timer),
    ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_cast(nodes_wanted_updated, State) ->
    do_nodes_wanted_updated(do_nodes_wanted()),
    {noreply, State};

handle_cast(_Msg, State)       -> {noreply, State}.

% Read from ns_config nodes_wanted, and add ourselves to the
% nodes_wanted list, if not already there.

handle_call(nodes_actual_proper, _From, State) ->
    {reply, do_nodes_actual_proper(), State};

handle_call(nodes_wanted, _From, State) ->
    {reply, do_nodes_wanted(), State};

handle_call(Msg, _From, State) ->
    error_logger:info_msg("Unhandled ~p call: ~p~n", [?MODULE, Msg]),
    {reply, error, State}.

handle_info({nodeup, Node}, State) ->
    ns_log:log(?MODULE, ?NODE_UP, "Node ~p saw that node ~p came up.",
               [node(), Node]),
    % We might be tempted to proactively push/pull/sync
    % our configs with the "new" Node.  Instead, it's
    % cleaner to asynchronous do a gen_event:notify()
    % Delay the notification for five seconds to give the
    % config a chance to settle before notifying clients.
    {ok, _Tref} = timer:send_after(?NODE_CHANGE_DELAY, notify_clients),
    %% Have every known node ping this new node.
    rpc:multicall(net_adm, ping, [Node]),
    {noreply, State};

handle_info({nodedown, Node}, State) ->
    ns_log:log(?MODULE, ?NODE_DOWN, "Node ~p saw that node ~p went down.",
               [node(), Node]),
    {ok, _Tref} = timer:send_after(?NODE_CHANGE_DELAY, notify_clients),
    {noreply, State};

handle_info(notify_clients, State) ->
    State2 = do_notify(State),
    {noreply, State2};

handle_info(ping_all, State) ->
    ping_all(),
    {noreply, State};

handle_info(_Msg, State) -> {noreply, State}.

% -----------------------------------------------------------

% Read from ns_config nodes_wanted.

do_nodes_wanted() ->
    case ns_config:search(ns_config:get(), nodes_wanted) of
        {value, L} -> lists:usort(L);
        false      -> []
    end.

%% The core of what happens when nodelists change
%% only used by do_nodes_wanted_updated
do_nodes_wanted_updated_fun(NodeListIn) ->
    {ok, Cookie} = cookie_sync(),
    NodeList = lists:usort(NodeListIn),
    error_logger:info_msg("ns_node_disco: nodes_wanted updated: ~p, with cookie: ~p~n",
                          [NodeList, erlang:get_cookie()]),
    erlang:set_cookie(node(), Cookie),
    PongList = lists:filter(fun(N) ->
                                    net_adm:ping(N) == pong
                            end,
                            NodeList),
    error_logger:info_msg("ns_node_disco: nodes_wanted pong: ~p, with cookie: ~p~n",
                          [PongList, erlang:get_cookie()]),
    ok.

%% Run do_nodes_wanted_updated_fun in a process, return the Pid.
do_nodes_wanted_updated(NodeListIn) ->
    spawn(fun() -> do_nodes_wanted_updated_fun(NodeListIn) end).

% Returns a subset of the nodes_wanted() that we see.  This is not the
% same as nodes([this, visible]) because this function may return a
% subset of nodes([this, visible]).  eg, many nodes might be visible
% at the OTP level.  But the caller only cares about the subset
% of nodes that are on the nodes_wanted() list.

do_nodes_actual_proper() ->
    Curr = nodes_actual(),
    Want = do_nodes_wanted(),
    Diff = lists:subtract(Curr, Want),
    lists:usort(lists:subtract(Curr, Diff)).

do_notify(#state{nodes = NodesOld} = State) ->
    NodesNew = do_nodes_actual_proper(),
    case NodesNew =:= NodesOld of
        true  -> State;
        false -> gen_event:notify(ns_node_disco_events,
                                  {ns_node_disco_events, NodesOld, NodesNew}),
                 State#state{nodes = NodesNew}
    end.

ping_all() ->
    lists:foreach(fun net_adm:ping/1, do_nodes_wanted()).

% -----------------------------------------------------------

cookie_gen() ->
    {A1, A2, A3} = erlang:now(),
    random:seed(A1, A2, A3),
    list_to_atom(misc:rand_str(16)).

cookie_init() ->
    NewCookie = cookie_gen(),
    ns_log:log(?MODULE, ?COOKIE_GEN, "Initial otp cookie generated: ~p",
               [NewCookie]),
    ok = cookie_set(NewCookie),
    {ok, NewCookie}.

% Sets our wanted otp cookie.
%
cookie_set(Cookie) ->
    X = ns_config:set(otp, [{cookie, Cookie}]),
    erlang:set_cookie(node(), Cookie),
    X.

% Gets our wanted otp cookie (might be =/= our actual otp cookie).
%
cookie_get() ->
    ns_config:search_prop(ns_config:get(), otp, cookie).

% Makes our wanted otp cookie =:= to our actual cookie.
% Will generate a cookie if needed for the first time.
%
cookie_sync() ->
    error_logger:info_msg("ns_node_disco cookie_sync~n"),
    case cookie_get() of
        undefined ->
            case erlang:get_cookie() of
                nocookie ->
                    % TODO: We should have length(nodes_wanted) == 0 or 1,
                    %       so, we should check that assumption.
                    cookie_init();
                CurrCookie ->
                    ns_log:log(?MODULE, ?COOKIE_INHERITED,
                               "Node ~p inherited otp cookie ~p from cluster",
                               [node(), CurrCookie]),
                    cookie_set(CurrCookie),
                    {ok, CurrCookie}
            end;
        WantedCookie ->
            case erlang:get_cookie() of
                WantedCookie -> {ok, WantedCookie};
                _ ->
                    ns_log:log(?MODULE, ?COOKIE_SYNCHRONIZED,
                               "Node ~p synchronized otp cookie ~p from cluster",
                               [node(), WantedCookie]),
                    erlang:set_cookie(node(), WantedCookie),
                    {ok, WantedCookie}
            end
    end.

ns_log_cat(?NODE_DOWN) ->
    warn;
ns_log_cat(_X) ->
    info.

ns_log_code_string(?COOKIE_INHERITED) ->
    "cookie update";
ns_log_code_string(?COOKIE_SYNCHRONIZED) ->
    "cookie update";
ns_log_code_string(?COOKIE_GEN) ->
    "cookie update";
ns_log_code_string(?NODE_UP) ->
    "node up";
ns_log_code_string(?NODE_DOWN) ->
    "node down".
