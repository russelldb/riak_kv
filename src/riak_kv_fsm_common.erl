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
         get_preflist/4,
         get_preflist/5,
         partition_local_remote/1,
         check_mailboxes/1,
         select_least_loaded_entry/2
        ]).

-export_type([preflist_entry/0, mbox_data/0]).

-type preflist_entry() :: {Idx::non_neg_integer(), node()}.
%% the information about vnode mailbox queues used to select a
%% coordinator

-opaque mbox_data() :: list(mbox_datum()).

-type mbox_datum() :: [{preflist_entry(),
                        MBoxSize::error | non_neg_integer(),
                        MBoxSofLimit:: error | non_neg_integer()}].
-type bkey() :: {riak_object:bucket(), riak_object:key()}.
-type fsm_options() :: riak_kv_get_fsm:options() | riak_kv_put_fsm:options().

-define(DEFAULT_MBOX_CHECK_TIMEOUT_MILLIS, 100).

%%%===================================================================
%%% API
%%%===================================================================

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

%% @doc determin request n_val from options and bucket props. Retunrs
%% `NVal::pos_integer()' or the error tuple `{error, {n_val_violation,
%% BadN::term()}}'
-spec get_n_val(fsm_options(), list()) ->
                       pos_integer() |
                       {error, {n_val_violation, BadN::term()}}.
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

%% @doc fetch an option by `Name' from a `Options' list, returns
%% option or `undefined' if `Name' is not present.
-spec get_option(atom(), fsm_options()) ->
                        term().
get_option(Name, Options) ->
    get_option(Name, Options, undefined).

%% @doc as get_option/2, but `Default' is returned if `Name' is not
%% present in `Options'.
-spec get_option(atom(), fsm_options(), term()) ->
                        term().
get_option(Name, Options, Default) ->
    case lists:keyfind(Name, 1, Options) of
        {_, Val} ->
            Val;
        false ->
            Default
    end.

-spec get_preflist(NVal::pos_integer(),
                   bkey(),
                   BucketProps::list(),
                   fsm_options()) ->
                          riak_core_apl:preflist_ann().
get_preflist(N, BKey, BucketProps, Options) ->
    get_preflist(N, BKey, BucketProps, Options, _BadCoodinators=[]).

-spec get_preflist(pos_integer(),
                   bkey(),
                   list(),
                   fsm_options(),
                   Bad::list(node())) ->
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

%% @doc Removes `Type::primary | fallback' info from preflist entries
%% and splits a preflist into local and remote entries.
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


%% @doc given local and remote mbox data, select the least loaded
%% preflist entry @TODO test to find the best strategy
-spec select_least_loaded_entry(list(mbox_data()), list(mbox_data())) ->
                                       {local, preflist_entry()} |
                                       {remote, preflist_entry()}.
select_least_loaded_entry([]=_LocalMboxData, RemoteMBoxData) ->
    [{Entry, _, _} | _Rest] = lists:sort(fun mbox_data_sort/2, RemoteMBoxData),
    {remote, Entry};
select_least_loaded_entry(LocalMBoxData, _RemoteMBoxData) ->
    [{Entry, _, _} | _Rest] = lists:sort(fun mbox_data_sort/2, LocalMBoxData),
    {local, Entry}.

%% @private used by select_least_loaded_entry/2 to sort mbox
%% data results
-spec mbox_data_sort(mbox_datum(), mbox_datum()) -> boolean().
mbox_data_sort({_, error, error}, {_, error, error}) ->
    %% @TODO do we use random here, so that a list full of errors
    %% picks a random node to forward to (as per pre-gh1661 code)?
    case random:uniform_s(2, os:timestamp()) of
        {1, _} ->
            true;
        {2, _} ->
            false
    end;
mbox_data_sort({_, error, error}, {_, _Load, _Limit}) ->
    false;
mbox_data_sort({_, _LoadA, _LimitA}, {_, error, error}) ->
    true;
mbox_data_sort({_, LoadA, _LimitA}, {_, LoadB, _LimitB})   ->
    %% @TODO should we randomise where load is equal? Or let load take
    %% care of it (i.e. is we always choose NodeA, it will get a
    %% longer queue and we will choose NodeB)
    LoadA =< LoadB.

%% @private check the mailboxes for the preflist.
-spec check_mailboxes(riak_core_apl:preflist()) ->
                             {true, preflist_entry()} |
                             {false, mbox_data()}.
check_mailboxes([]) ->
    %% shortcut for empty (local)preflist
    {false, []};
check_mailboxes(Preflist) ->
    TimeLimit = get_timestamp_millis() + ?DEFAULT_MBOX_CHECK_TIMEOUT_MILLIS,
    _ = [check_mailbox(Entry) || Entry <- Preflist],
    case join_mbox_replies(length(Preflist),
                           TimeLimit,
                           []) of
        {true, Entry} ->
            {true, Entry};
        {ok, MBoxData} ->
            %% all replies in, none are below soft-limit
            {false, MBoxData};
        {timeout, MBoxData0} ->
            lager:warning("Mailbox soft-load poll timout ~p",
                          [?DEFAULT_MBOX_CHECK_TIMEOUT_MILLIS]),
            MBoxData = add_errors_to_mbox_data(Preflist, MBoxData0),
            {false, MBoxData}
    end.

%% @private cast off to vnode proxy for mailbox size.
-spec check_mailbox(preflist_entry()) -> ok.
check_mailbox({Idx, Node}=Entry) ->
    RegName = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx, Node),
    riak_core_vnode_proxy:cast(RegName, {mailbox_size, self(), Entry}).

%% @private wait at most `TimeOutMillis' for `HowMany' `{mbox, _}'
%% replies from riak_core_vnode_proxy. Returns either `Acc' as mbox
%% data or short-circuits to `{true, Entry}' if any proxy replies
%% below the soft limit.
-spec join_mbox_replies(NumReplies::non_neg_integer(),
                        Timeout::pos_integer(), Acc::mbox_data()) ->
                               {true, preflist_entry()} |
                               {timeout, mbox_data()} |
                               {ok, mbox_data()}.
join_mbox_replies(0, _TimeLimit, Acc) ->
    {ok, Acc};
join_mbox_replies(HowMany, TimeLimit, Acc) ->
    TimeOut = max(0, TimeLimit - get_timestamp_millis()),
    receive
        {mbox, {Entry, {ok, _Mbox, _Limit}}} ->
            %% shortcut it
            {true, Entry};
        {mbox, {Entry, {soft_loaded, MBox, Limit}}} ->
            %% Keep waiting @TODO warning? Over 2million in 24h load test!
            lager:debug("Mailbox for ~p with soft-overload", [Entry]),
            join_mbox_replies(HowMany-1, TimeLimit,
                 [{Entry, MBox, Limit} | Acc])
    after TimeOut ->
            {timeout, Acc}
    end.

%% @private in the case that some preflist entr(y|ies) did not respond
%% in time, add them to the mbox data result as `error' entries.
-spec add_errors_to_mbox_data(riak_core_apl:preflist(), mbox_data()) ->
                                     mbox_data().
add_errors_to_mbox_data(Preflist, Acc) ->
    lists:map(fun(Entry) ->
                      case lists:keyfind(Entry, 1, Acc) of
                          false ->
                              lager:warning("Mailbox for ~p did not return in time", [Entry]),
                              {Entry, error, error};
                          Res ->
                              Res
                      end
              end, Preflist).

-spec get_timestamp_millis() -> pos_integer().
get_timestamp_millis() ->
  {Mega, Sec, Micro} = os:timestamp(),
  (Mega*1000000 + Sec)*1000 + round(Micro/1000).

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

mbox_data_sort_test() ->
    ?assert(mbox_data_sort({x, 1000, 10}, {x, error, error})),
    ?assertNot(mbox_data_sort({x, error, error}, {x, 100, 10})),
    ?assertNot(mbox_data_sort({x, 10, 10}, {x, 1, 10})),
    ?assert(mbox_data_sort({x, 10, 10}, {x, 10, 10})),
    ?assert(mbox_data_sort({x, 1, 10}, {x, 10, 10})).

select_least_loaded_entry_test() ->
    MBoxData = [{{0, nodex}, 100, 10},
                {{1919191919, nodey}, 10, 10},
                {{91829, nodez}, error, error},
                {{100, nodea}, 1, 10},
                {{90, nodeb}, error, error},
                {{182, c}, 10001, 10}],
    ?assertEqual({local, {100, nodea}}, select_least_loaded_entry(MBoxData, [])),
    ?assertEqual({remote, {100, nodea}}, select_least_loaded_entry([], MBoxData)),
    ?assertEqual({local, {100, nodea}}, select_least_loaded_entry(MBoxData, MBoxData)).


partition_local_remote_test() ->
    Node = node(),
    PL1 = [
           {{0, 'node0'}, primary},
           {{1, Node}, fallback},
           {{2, 'node1'}, primary}
          ],
    ?assertEqual({
                   [{1, Node}],
                   [{2, 'node1'},
                    {0, 'node0'}]
                 }, partition_local_remote(PL1)),

    PL2 = [
           {{0, 'node0'}, primary},
           {{1, Node}, fallback},
           {{2, Node}, primary},
           {{3, 'node0'}, fallback}
          ],

    ?assertEqual({
                   [{2, Node},
                    {1, Node}],
                   [{3, 'node0'},
                    {0, 'node0'}]
                 }, partition_local_remote(PL2)),
    ?assertEqual({[], []}, partition_local_remote([])),
    ?assertEqual({[], [{3, 'node0'},
                       {0, 'node0'}]}, partition_local_remote([
                                                               {{0, 'node0'}, primary},
                                                               {{3, 'node0'}, fallback}
                                                              ])),
    ?assertEqual({[{1, Node}], []}, partition_local_remote([
                                                            {{1, Node}, primary}
                                                           ])).

-endif.




