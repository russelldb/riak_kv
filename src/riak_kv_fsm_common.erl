%% -------------------------------------------------------------------
%%
%% riak_kv_fsm_common: slight refactor of some common code from the
%% riak get/put fsms
%%
%% Copyright (c) 2007-2018 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_kv_fsm_common).
-include_lib("riak_kv_vnode.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
         get_bucket_props/1,
         get_n_val/2,
         get_option/2,
         get_option/3,
         get_preflist/5,
         partition_local_remote/1
        ]).

-type bkey() :: {riak_object:bucket(), riak_object:key()}.

-spec get_bucket_props(riak_object:bucket()) -> list().
get_bucket_props(Bucket) ->
    {ok, DefaultProps} = application:get_env(riak_core,
                                             default_bucket_props),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    %% typed buckets never fall back to defaults
    case is_tuple(Bucket) of
        false ->
            lists:keymerge(1, lists:keysort(1, BucketProps),
                           lists:keysort(1, DefaultProps));
        true ->
            BucketProps
    end.

%% @private decide on the N Val for the put request, and error if
%% there is a violation.
get_n_val(Options, BucketProps) ->
    Bucket_N = get_option(n_val, BucketProps),
    case get_option(n_val, Options, false) of
        false ->
            Bucket_N;
        N_val when is_integer(N_val), N_val > 0, N_val =< Bucket_N ->
            %% don't allow custom N to exceed bucket N
            N_val;
        Bad_N ->
            {error, {n_val_violation, Bad_N}}
    end.

get_option(Name, Options) ->
    get_option(Name, Options, undefined).

get_option(Name, Options, Default) ->
    case lists:keyfind(Name, 1, Options) of
        {_, Val} ->
            Val;
        false ->
            Default
    end.

-spec get_preflist(pos_integer(),
                   bkey(),
                   list(),
                   riak_kv_get_fsm:options() | riak_kv_put_fsm:options(),
                   list(node())) ->
                          riak_core_apl:preflist_ann().
get_preflist(N, BKey, BucketProps, Options, BadCoordinators) ->
    DocIdx = riak_core_util:chash_key(BKey, BucketProps),

    case get_option(sloppy_quorum, Options, true) of
        true ->
            UpNodes = riak_core_node_watcher:nodes(riak_kv),
            riak_core_apl:get_apl_ann(DocIdx, N,
                                      UpNodes -- BadCoordinators);
        false ->
            Preflist0 =
                riak_core_apl:get_primary_apl(DocIdx, N, riak_kv),
            [X || X = {{_Index, Node}, _Type} <- Preflist0,
                  not lists:member(Node, BadCoordinators)]
    end.

%% @private used by `coordinate_or_forward' above. Removes `Type' info
%% from preflist entries and splits a preflist into local and remote.
-spec partition_local_remote(riak_core_apl:preflist_ann()) ->
                                    {riak_core_apl:preflist(),
                                     riak_core_apl:preflist()}.
partition_local_remote(Preflist) ->
    lists:foldl(fun partition_local_remote/2,
                {[], []},
                Preflist).

%% @private fold fun for `partition_local_remote/1'
partition_local_remote({{_Index, Node}=IN, _Type}, {L, R})
  when Node == node() ->
    {[IN | L], R};
partition_local_remote({IN, _Type}, {L, R}) ->
    {L, [IN | R]}.


-ifdef(TEST).

get_bucket_props_test_() ->
    BucketProps = [{bprop1, bval1},
                   {bprop2, bval2},
                   {prop2, bval9},
                   {prop1, val1}],
    DefaultProps = [{prop1, val1},
                    {prop2, val2},
                    {prop3, val3}],
    {setup, fun() ->
                    DefPropsOrig = application:get_env(riak_core,
                                                       default_bucket_props),
                    application:set_env(riak_core,
                                        default_bucket_props,
                                        DefaultProps),

                    meck:new(riak_core_bucket),
                    meck:expect(riak_core_bucket, get_bucket,
                                fun(_) ->
                                        BucketProps
                                end),

                    DefPropsOrig
            end,
     fun(DefPropsOrig) ->
             application:set_env(riak_core,
                                 default_bucket_props,
                                 DefPropsOrig),
             meck:unload(riak_core_bucket)

     end,
     [{"untyped bucket",
       ?_test(
          begin
              Bucket = <<"bucket">>,
              %% amazing, not a ukeymerge
              Props = get_bucket_props(Bucket),
              %% i.e. a merge with defaults
              ?assertEqual([
                            {bprop1,bval1},
                            {bprop2,bval2},
                            {prop1,val1},
                            {prop1,val1},
                            {prop2,bval9},
                            {prop2,val2},
                            {prop3,val3}
                           ], Props)
          end)
      },
      {"typed bucket",
       ?_test(
          begin
              Bucket = {<<"type">>, <<"bucket">>},
              Props = get_bucket_props(Bucket),
              %% i.e. no merge with defaults
              ?assertEqual(BucketProps, Props)
          end
         )
      }
     ]}.

get_n_val_test() ->
    Opts0 = [],
    BProps = [{n_val, 3}, {other, props}],
    ?assertEqual(3, get_n_val(Opts0, BProps)),
    Opts1 = [{n_val, 1}],
    ?assertEqual(1, get_n_val(Opts1, BProps)),
    Opts2 = [{n_val, 4}],
    ?assertEqual({error, {n_val_violation, 4}}, get_n_val(Opts2, BProps)).
-endif.




