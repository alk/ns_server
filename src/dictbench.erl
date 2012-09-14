-module(dictbench).

-export([bench_N/1, bench_dict_N/1]).

fetch_keyfind(Dict, K) ->
    case lists:keyfind(K, 1, Dict) of
        {_, V} ->
            {ok, V};
        false ->
            error
    end.

bench_keyfind_outer(_Dict, 0) ->
    ok;
bench_keyfind_outer(Dict, Iterations) ->
    bench_keyfind_inner(Dict, 10, 15),
    bench_keyfind_outer(Dict, Iterations-1).

bench_keyfind_inner(_Dict, K, EndK) when K =:= EndK ->
    ok;
bench_keyfind_inner(Dict, K, EndK) ->
    fetch_keyfind(Dict, K),
    bench_keyfind_inner(Dict, K+1, EndK).


bench_orddict_outer(_Dict, 0) ->
    ok;
bench_orddict_outer(Dict, Iterations) ->
    bench_orddict_inner(Dict, 10, 20),
    bench_orddict_outer(Dict, Iterations-1).

bench_orddict_inner(_Dict, K, EndK) when K =:= EndK ->
    ok;
bench_orddict_inner(Dict, K, EndK) ->
    orddict:find(K, Dict),
    bench_orddict_inner(Dict, K+1, EndK).

bench_N(N) ->
    From = 10 - N div 2,
    To = From + N,
    Dict = [{I, I} || I <- lists:seq(From, To)],
    io:format("Benching size: ~p~n", [N]),
    io:format("Keyfind: "),
    {T1, _} = timer:tc(fun bench_keyfind_outer/2, [Dict, 10000]),
    io:format("~p~n", [T1]),
    io:format("orddict: "),
    {T2, _} = timer:tc(fun bench_orddict_outer/2, [Dict, 10000]),
    io:format("~p~n", [T2]).

bench_dict_outer(_Dict, 0) ->
    ok;
bench_dict_outer(Dict, Iterations) ->
    bench_dict_inner(Dict, 10, 20),
    bench_dict_outer(Dict, Iterations-1).

bench_dict_inner(_Dict, K, EndK) when K =:= EndK ->
    ok;
bench_dict_inner(Dict, K, EndK) ->
    dict:find(K, Dict),
    bench_dict_inner(Dict, K+1, EndK).

bench_dict_N(N) ->
    From = 10 - N div 2,
    To = From + N,
    PreDict = [{I, I} || I <- lists:seq(From, To)],
    Dict = dict:from_list(PreDict),
    io:format("dict: Benching size: ~p~n", [N]),
    {T1, _} = timer:tc(fun bench_dict_outer/2, [Dict, 10000]),
    io:format("dict: ~p~n", [T1]).
