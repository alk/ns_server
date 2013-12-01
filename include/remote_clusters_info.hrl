-record(remote_node, {host :: string(),
                      port :: integer(),
                      ssl_proxy_port :: pos_integer() | undefined | needed_but_unknown}).

-record(remote_cluster, {uuid :: binary(),
                         nodes :: [#remote_node{}],
                         cert :: binary() | undefined
                        }).

-type remote_bucket_node() :: {Host :: binary(), MCDPort :: integer()}.

-record(remote_bucket, {uuid :: binary(),
                        password :: binary(),
                        cluster_uuid :: binary(),
                        server_list :: [remote_bucket_node()],
                        raw_vbucket_map :: dict(),
                        capi_vbucket_map :: dict()}).
