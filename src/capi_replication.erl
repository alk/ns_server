%% @author Couchbase <info@couchbase.com>
%% @copyright 2011 Couchbase, Inc.
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

-module(capi_replication).

-export([get_missing_revs/2, update_replicated_docs/3]).

%% those are referenced from capi.ini
-export([handle_pre_replicate/1, handle_commit_for_checkpoint/1]).

-include("xdc_replicator.hrl").
-include("mc_entry.hrl").
-include("mc_constants.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(SLOW_THRESHOLD_SECONDS, 180).

%% note that usual REST call timeout is 60 seconds. So no point making it any longer
-define(XDCR_CHECKPOINT_TIMEOUT, ns_config:get_timeout_fast(xdcr_checkpoint_timeout, 60000)).


%% public functions
get_missing_revs(#db{name = DbName}, JsonDocIdRevs) ->
    {Bucket, VBucket} = capi_utils:split_dbname(DbName),
    TimeStart = now(),
    %% enumerate all keys and fetch meta data by getMeta for each of them to ep_engine
    Results =
        lists:foldr(
          fun ({Id, Rev}, Acc) ->
                  case is_missing_rev(Bucket, VBucket, Id, Rev) of
                      false ->
                          Acc;
                      true ->
                          [{Id, Rev} | Acc]
                  end;
              (_, _) ->
                  throw(unsupported)
          end, [], JsonDocIdRevs),

    NumCandidates = length(JsonDocIdRevs),
    RemoteWinners = length(Results),
    TimeSpent = timer:now_diff(now(), TimeStart) div 1000,
    AvgLatency = TimeSpent div NumCandidates,
    ?xdcr_debug("[Bucket:~p, Vb:~p]: after conflict resolution for ~p docs, num of remote winners is ~p and "
                "number of local winners is ~p. (time spent in ms: ~p, avg latency in ms per doc: ~p)",
                [Bucket, VBucket, NumCandidates, RemoteWinners, (NumCandidates-RemoteWinners),
                 TimeSpent, AvgLatency]),

    %% dump error msg if timeout
    TimeSpentSecs = TimeSpent div 1000,
    case TimeSpentSecs > ?SLOW_THRESHOLD_SECONDS of
        true ->
            ?xdcr_error("[Bucket:~p, Vb:~p]: conflict resolution for ~p docs  takes too long to finish!"
                        "(total time spent: ~p secs)",
                        [Bucket, VBucket, NumCandidates, TimeSpentSecs]);
        _ ->
            ok
    end,

    {ok, Results}.

update_replicated_docs(#db{name = DbName}, Docs, Options) ->
    {Bucket, VBucket} = capi_utils:split_dbname(DbName),

    case proplists:get_value(all_or_nothing, Options, false) of
        true ->
            throw(unsupported);
        false ->
            ok
    end,

    TimeStart = now(),
    %% enumerate all docs and update them
    Errors =
        lists:foldr(
          fun (#doc{id = Id, rev = Rev} = Doc, ErrorsAcc) ->
                  case do_update_replicated_doc_loop(Bucket, VBucket, Doc) of
                      ok ->
                          ErrorsAcc;
                      {error, Error} ->
                          [{{Id, Rev}, Error} | ErrorsAcc]
                  end
          end,
          [], Docs),

    TimeSpent = timer:now_diff(now(), TimeStart) div 1000,
    AvgLatency = TimeSpent div length(Docs),

    %% dump error msg if timeout
    TimeSpentSecs = TimeSpent div 1000,
    case TimeSpentSecs > ?SLOW_THRESHOLD_SECONDS of
        true ->
            ?xdcr_error("[Bucket:~p, Vb:~p]: update ~p docs takes too long to finish!"
                        "(total time spent: ~p secs)",
                        [Bucket, VBucket, length(Docs), TimeSpentSecs]);
        _ ->
            ok
    end,

    case Errors of
        [] ->
            ?xdcr_debug("[Bucket:~p, Vb:~p]: successfully update ~p replicated mutations "
                        "(time spent in ms: ~p, avg latency per doc in ms: ~p)",
                        [Bucket, VBucket, length(Docs), TimeSpent, AvgLatency]),

            ok;
        [FirstError | _] ->
            %% for some reason we can only return one error. Thus
            %% we're logging everything else here
            ?xdcr_error("[Bucket: ~p, Vb: ~p] Error: could not update docs. Time spent in ms: ~p, "
                        "# of docs trying to update: ~p, error msg: ~n~p",
                        [Bucket, VBucket, TimeSpent, length(Docs), Errors]),
            {ok, FirstError}
    end.

%% helper functions
is_missing_rev(Bucket, VBucket, Id, RemoteMeta) ->
    case get_meta(Bucket, VBucket, Id) of
        {memcached_error, key_enoent, _CAS} ->
            true;
        {memcached_error, not_my_vbucket, _} ->
            throw({bad_request, not_my_vbucket});
        {ok, LocalMeta, _CAS} ->
             %% we do not have any information about deletedness of
             %% the remote side thus we use only revisions to
             %% determine a winner
            case max(LocalMeta, RemoteMeta) of
                %% if equal, prefer LocalMeta since in this case, no need
                %% to replicate the remote item, hence put LocalMeta before
                %% RemoteMeta.
                LocalMeta ->
                    false;
                RemoteMeta ->
                    true
            end
    end.

do_update_replicated_doc_loop(Bucket, VBucket, Doc0) ->
    Doc = #doc{id = DocId, rev = DocRev,
        body = DocValue, deleted = DocDeleted} = couch_doc:with_json_body(Doc0),
    {DocSeqNo, DocRevId} = DocRev,
    RV =
        case get_meta(Bucket, VBucket, DocId) of
            {memcached_error, key_enoent, CAS} ->
                update_locally(Bucket, DocId, VBucket, DocValue, DocRev, DocDeleted, CAS);
            {memcached_error, not_my_vbucket, _} ->
                {error, {bad_request, not_my_vbucket}};
            {ok, {OurSeqNo, OurRevId}, LocalCAS} ->
                {RemoteMeta, LocalMeta} =
                    case DocDeleted of
                        false ->
                            %% for non-del mutation, compare full metadata
                            {{DocSeqNo, DocRevId}, {OurSeqNo, OurRevId}};
                        _ ->
                            %% for deletion, just compare seqno and CAS to match
                            %% the resolution algorithm in ep_engine:deleteWithMeta
                            <<DocCAS:64, _DocExp:32, _DocFlg:32>> = DocRevId,
                            <<OurCAS:64, _OurExp:32, _OurFlg:32>> = OurRevId,
                            {{DocSeqNo, DocCAS}, {OurSeqNo, OurCAS}}
                    end,
                case max(LocalMeta, RemoteMeta) of
                    %% if equal, prefer LocalMeta since in this case, no need
                    %% to replicate the remote item, hence put LocalMeta before
                    %% RemoteMeta.
                    LocalMeta ->
                        ok;
                    %% if remoteMeta wins, need to persist the remote item, using
                    %% the same CAS returned from the get_meta() above.
                    RemoteMeta ->
                        update_locally(Bucket, DocId, VBucket, DocValue, DocRev, DocDeleted, LocalCAS)
                end
        end,

    case RV of
        retry ->
            do_update_replicated_doc_loop(Bucket, VBucket, Doc);
        _Other ->
            RV
    end.

update_locally(Bucket, DocId, VBucket, Value, Rev, DocDeleted, LocalCAS) ->
    case ns_memcached:update_with_rev(Bucket, VBucket, DocId, Value, Rev, DocDeleted, LocalCAS) of
        {ok, _, _} ->
            ok;
        {memcached_error, key_enoent, _} ->
            retry;
        {memcached_error, key_eexists, _} ->
            retry;
        {memcached_error, not_my_vbucket, _} ->
            {error, {bad_request, not_my_vbucket}};
        {memcached_error, einval, _} ->
            {error, {bad_request, einval}}
    end.


get_meta(Bucket, VBucket, DocId) ->
    case ns_memcached:get_meta(Bucket, DocId, VBucket) of
        {ok, Rev, CAS, _MetaFlags} ->
            {ok, Rev, CAS};
        Other ->
            Other
    end.

%% NOTE: only used by old code path
generate_local_vbopaque(Bucket, VBucket) ->
    StartupTime = ns_memcached:get_ep_startup_time_for_xdcr(Bucket),
    VBUUID = xdc_vbucket_rep_ckpt:get_local_vbuuid(Bucket, VBucket),
    [VBUUID, StartupTime].

extract_ck_params(Req) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Obj} = couch_httpd:json_body_obj(Req),
    Bucket =
        case proplists:get_value(<<"bucket">>, Obj) of
            undefined ->
                undefined;
            B ->
                %% pre 3.0 clusters will send us "dbname+uuid" here
                %% so parse uuid out
                {B1, undefined, _} = capi_utils:split_dbname_with_uuid(B),
                B1
        end,

    VB = proplists:get_value(<<"vb">>, Obj),
    BucketUUID = proplists:get_value(<<"bucketUUID">>, Obj),
    case (Bucket =:= undefined
          orelse VB =:= undefined
          orelse BucketUUID =:= undefined) of
        true ->
            erlang:throw(not_found);
        _ -> true
    end,
    BucketConfig = capi_frontend:verify_bucket_auth(Req, Bucket),

    case proplists:get_value(uuid, BucketConfig) =:= BucketUUID of
        true ->
            ok;
        false ->
            erlang:throw({not_found, uuid_mismatch})
    end,

    VBOpaque = proplists:get_value(<<"vbopaque">>, Obj),

    CommitOpaque = proplists:get_value(<<"commitopaque">>, Obj),

    {Bucket, VB, VBOpaque, CommitOpaque}.

handle_pre_replicate(Req) ->
    handle_pre_replicate_old(Req).

handle_pre_replicate_old(#httpd{method='POST'}=Req) ->
    {Bucket, VB, VBOpaque, CommitOpaque} = extract_ck_params(Req),

    [LocalCommitOpaque, _] = LocalVBOpaque = generate_local_vbopaque(Bucket, VB),

    VBMatches = VBOpaque =:= undefined orelse VBOpaque =:= LocalVBOpaque,

    CommitOk = CommitOpaque =:= undefined orelse CommitOpaque =:= LocalCommitOpaque,

    ?log_debug("Old pre-replicate: ~p", [{VBOpaque, LocalVBOpaque, CommitOpaque, LocalCommitOpaque}]),

    Code = case VBMatches andalso CommitOk of
               true -> 200;
               false -> 400
           end,

    couch_httpd:send_json(Req, Code, {[{<<"vbopaque">>, LocalVBOpaque}]}).


handle_commit_for_checkpoint(#httpd{method='POST'}=Req) ->
    handle_commit_for_checkpoint_old(Req).

handle_commit_for_checkpoint_old(#httpd{method='POST'}=Req) ->
    {Bucket, VB, VBOpaque, _} = extract_ck_params(Req),

    TimeBefore = erlang:now(),
    system_stats_collector:increment_counter(xdcr_checkpoint_commits_enters, 1),
    try
        ok = ns_memcached:perform_checkpoint_commit_for_xdcr(Bucket, VB, ?XDCR_CHECKPOINT_TIMEOUT)
    after
        system_stats_collector:increment_counter(xdcr_checkpoint_commits_leaves, 1)
    end,

    TimeAfter = erlang:now(),
    system_stats_collector:add_histo(xdcr_checkpoint_commit_time, timer:now_diff(TimeAfter, TimeBefore)),
    [CommitOpaque, _] = LocalVBOpaque = generate_local_vbopaque(Bucket, VB),

    CommitOpaque = hd(LocalVBOpaque),
    case LocalVBOpaque =:= VBOpaque of
        true ->
            system_stats_collector:increment_counter(xdcr_checkpoint_commit_oks, 1),
            couch_httpd:send_json(Req, 200, {[{<<"commitopaque">>, CommitOpaque}]});
        _ ->
            system_stats_collector:increment_counter(xdcr_checkpoint_commit_mismatches, 1),
            couch_httpd:send_json(Req, 400, {[{<<"vbopaque">>, LocalVBOpaque}]})
    end.

validate_commit(FailoverLog, CommitUUID, CommitSeq) ->
    {FailoverUUIDs, FailoverSeqs} = lists:unzip(FailoverLog),

    [SeqnosStart | FailoverSeqs1] = FailoverSeqs,

    %% validness failover log is where each uuid entry has seqno where
    %% it _ends_ rather than where it begins. It makes validness
    %% checking simpler
    ValidnessFailoverLog = lists:zip(FailoverUUIDs, FailoverSeqs1 ++ [16#ffffffffffffffff]),

    case SeqnosStart > CommitSeq of
        true -> false;
        _ ->
            lists:any(fun ({U, EndSeq}) ->
                              U =:= CommitUUID andalso CommitSeq =< EndSeq
                      end, ValidnessFailoverLog)
    end.

validate_commit_test() ->
    FailoverLog = [{13685158163256569856, 0},
                   {4598340681889701145, 48}],
    CommitUUID = 13685158163256569445,
    CommitSeq = 27,
    true = not validate_commit(FailoverLog, CommitUUID, CommitSeq),
    true = validate_commit(FailoverLog, 13685158163256569856, CommitSeq).
