%%%-------------------------------------------------------------------
%%% @author Northscale <info@northscale.com>
%%% @copyright 2010 NorthScale, Inc.
%%% All rights reserved.
%%% @doc
%%%
%%% This module handles erlang node name (and IP address)
%%% configuration. It sets erlang name only when it wasn't set
%%% before. IP address is kept in <config directory>/ip file. Running
%%% several nodes per machine is allowed by passing -name explicitly.
%%%
%%% @end
%%% Created : 14 Jun 2010 by Aliaksey Kandratsenka <alk@tut.by>
%%%-------------------------------------------------------------------
-module(address_manager).

-behaviour(gen_server).

%% API
-export([start_link/1, set_ip_address/1, do_restart_everything/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {ip_config_path, have_address}).

%%%===================================================================
%%% API
%%%===================================================================

set_ip_address(IPAddress) ->
    gen_server:call(?MODULE, {set_ip_address, IPAddress}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(CfgPath) ->
    io:format("address_manager:enter~n"),
    RV = gen_server:start_link({local, ?SERVER}, ?MODULE, [CfgPath], []),
    io:format("address_manager:exit~n"),
    RV.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([CfgPath]) ->
    Path = filename:join(filename:dirname(CfgPath), "ip"),
    Locked = is_current_name_locked(),
    HaveAddress = case Locked of
                      true -> true;
                      false ->
                          {HA, Addr} = case read_address_config(Path) of
                                           {ok, IPAddress} ->
                                               {true, IPAddress};
                                           _ ->
                                               {false, guess_best_ip_address()}
                                       end,
                          start_network(Addr),
                          HA
                  end,
    io:format("address_manager:init completion~n"),
    {ok, #state{ip_config_path = Path,
                have_address = HaveAddress}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({set_ip_address, IPAddress}, _, State) ->
    case is_current_name_locked() of
        true ->
            {reply, locked, State};
        false ->
            case verify_address(IPAddress) of
                {error, _} = Crap ->
                    {reply, Crap, State};
                ok ->
                    ok = save_address_config(State#state.ip_config_path, IPAddress),
                    OldNode = node(),
                    io:format("setting ip address: ~p~n", [IPAddress]),
                    ok = start_network(IPAddress),
                    NewNode = node(),
                    io:format("new node name is ~p~n", [NewNode]),
                    ns_config:update(fun ({node, Node, X}) when Node =:= OldNode -> {node, NewNode, X};
                                         (X) -> X
                                     end, make_ref()),
                    {ok, _} = timer:apply_after(1000, ?MODULE, do_restart_everything, []),
                    {reply, ok, State#state{have_address=true}}
            end
    end;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

do_restart_everything() ->
    io:format("re-spawning ns_cluster!~n"),
    ns_cluster:leave_sync().

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

verify_address(AddrString) ->
    case inet:getaddr(AddrString, inet) of
        {error, Errno1} = Crap1 ->
            error_logger:error_msg("Got error:~p. Ignoring bad address:~s~n", [Errno1, AddrString]),
            Crap1;
        {ok, IpAddr} ->
            case gen_tcp:listen(0, [inet, {ip, IpAddr}]) of
                {error, Errno2} = Crap2 ->
                    error_logger:error_msg("Got error:~p. Cannot listen on configured address:~s~n", [Errno2, AddrString]),
                    Crap2;
                {ok, Socket} ->
                    gen_tcp:close(Socket),
                    ok
            end
    end.

read_address_config(Path) ->
    error_logger:info_msg("reading ip config from ~p~n", [Path]),
    case file:read_file(Path) of
        {ok, BinaryContents} ->
            AddrString = string:strip(binary_to_list(BinaryContents)),
            case verify_address(AddrString) of
                ok -> AddrString;
                _ -> undefined
            end;
        _ -> undefined
    end.

save_address_config(Path, IPAddress) ->
    error_logger:info_msg("saving ip config to ~p~n", [Path]),
    TmpPath = Path ++ ".tmp",
    case file:write_file(TmpPath, IPAddress) of
        ok ->
            file:rename(TmpPath, Path);
        X -> X
    end.

start_network(IPAddress) ->
    MyNodeName = list_to_atom("ns_1@" ++ IPAddress),
    net_kernel:stop(),
    {ok, _Pid} = net_kernel:start([MyNodeName, longnames]),
    ns_config:set(nodes_wanted, [node()]),
    ok.

address_badness({127, _, _, _}) -> %% localhost
    3;
address_badness({169, 254, _, _}) -> %% link-local
    2;
address_badness({10, _, _, _}) -> %% private
    1;
address_badness({172, X, _, _}) when X >= 16 andalso X < 32 -> %% private
    1;
address_badness({192, 168, _, _}) -> %% private
    1;
address_badness(_) -> %% everything else is good
    0.

pick_best_address_rec([], Best, _BestBadness) ->
    Best;
pick_best_address_rec([H | T], Best, BestBadness) ->
    Badness = address_badness(H),
    case Badness of
        0 -> H;
        _ ->
            case Badness < BestBadness of
                true -> pick_best_address_rec(T, H, Badness);
                _ -> pick_best_address_rec(T, Best, BestBadness)
            end
    end.

pick_best_address([H | T]) ->
    pick_best_address_rec(T, H, address_badness(H)).

is_current_name_locked() ->
    case ns_config:search_node(otp_name_locked) of
        false -> false;
        {value, X} -> X
    end.

guess_best_ip_address() ->
    {ok, AddrInfo} = inet:getif(),
    AddrList = lists:map(fun ({A,_,_}) -> A end, AddrInfo),
    Addr = pick_best_address(AddrList),
    string:join(lists:map(fun erlang:integer_to_list/1,
                          tuple_to_list(Addr)),
                ".").
