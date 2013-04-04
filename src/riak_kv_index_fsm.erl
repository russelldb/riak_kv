%% -------------------------------------------------------------------
%%
%% riak_index_fsm: Manage secondary index queries.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The index fsm manages the execution of secondary index queries.
%%
%%      The index fsm creates a plan to achieve coverage
%%      of the cluster using the minimum
%%      possible number of VNodes, sends index query
%%      commands to each of those VNodes, and compiles the
%%      responses.
%%
%%      The number of VNodes required for full
%%      coverage is based on the number
%%      of partitions, the number of available physical
%%      nodes, and the bucket n_val.

-module(riak_kv_index_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         plan/2,
         process_results/3,
         process_results/2,
         finish/2]).
-export([use_ack_backpressure/0,
         req/3]).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {from :: from(),
                merge_sort_buffer :: sms:sms(),
                max_results :: all | pos_integer(),
                results_sent = 0 :: non_neg_integer()}).

%% @doc Returns `true' if the new ack-based backpressure index
%% protocol should be used.  This decision is based on the
%% `index_backpressure' setting in `riak_kv''s application
%% environment.
-spec use_ack_backpressure() -> boolean().
use_ack_backpressure() ->
    riak_core_capability:get({riak_kv, index_backpressure}, false) == true.

%% @doc Construct the correct index command record.
-spec req(binary(), term(), term()) -> term().
req(Bucket, ItemFilter, Query) ->
    case use_ack_backpressure() of
        true ->
            ?KV_INDEX_REQ{bucket=Bucket,
                          item_filter=ItemFilter,
                          qry=Query};
        false ->
            #riak_kv_index_req_v1{bucket=Bucket,
                                  item_filter=ItemFilter,
                                  qry=Query}
    end.

%% @doc Return a tuple containing the ModFun to call per vnode,
%% the number of primary preflist vnodes the operation
%% should cover, the service to use to check for available nodes,
%% and the registered name to use to access the vnode master process.
init(From={_, _, _}, [Bucket, ItemFilter, Query, Timeout]) ->
    %% http://erlang.org/doc/reference_manual/expressions.html#id77404
    %% atom() > number()
    init(From, [Bucket, ItemFilter, Query, Timeout, all]);
init(From={_, _, _}, [Bucket, ItemFilter, Query, Timeout, MaxResults]) ->
    %% Get the bucket n_val for use in creating a coverage plan
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),
    %% Construct the key listing request
    Req = req(Bucket, ItemFilter, Query),
    {Req, all, NVal, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{from=From, max_results=MaxResults}}.

plan(CoverageVNodes, State) ->
    {ok, State#state{merge_sort_buffer=sms:new(CoverageVNodes)}}.

process_results(_VNode, {error, Reason}, _State) ->
    {error, Reason};
process_results(_VNode, {From, _Bucket, _Results}, State=#state{max_results=X, results_sent=Y})  when Y >= X ->
    riak_kv_vnode:stop_fold(From),
    {done, State};
process_results(VNode, {From, Bucket, Results}, State) ->
    {ok, State2} = process_results(VNode, {Bucket, Results}, State),
    riak_kv_vnode:ack_keys(From),
    {ok, State2};
process_results(VNode, {_Bucket, Results}, State) ->
    #state{merge_sort_buffer=MergeSortBuffer,
           from={raw, ReqId, ClientPid}, results_sent=ResultsSent, max_results=MaxResults} = State,
    %% TODO: What about MR, does it need / care for this
    %% add new results to buffer
    BufferWithNewResults = sms:add_results(VNode, lists:reverse(Results), MergeSortBuffer),
    ProcessBuffer = sms:sms(BufferWithNewResults),
    {NewBuffer, Sent} = case ProcessBuffer of
                            {[], BufferWithNewResults} ->
                                {BufferWithNewResults, 0};
                            {ToSend, NewBuff} ->
                                DownTheWire = case (ResultsSent + length(ToSend)) > MaxResults of
                                                  true ->
                                                      lists:sublist(ToSend, MaxResults - ResultsSent);
                                                  false ->
                                                      ToSend
                                              end,
                                ClientPid ! {ReqId, {results, DownTheWire}},
                                {NewBuff, length(DownTheWire)}
                        end,
    {ok, State#state{merge_sort_buffer=NewBuffer, results_sent=Sent+ResultsSent}};
process_results(VNode, done, State) ->
    %% tell the sms buffer about the done vnode
    #state{merge_sort_buffer=MergeSortBuffer} = State,
    BufferWithNewResults = sms:add_results(VNode, done, MergeSortBuffer),
    {done, State#state{merge_sort_buffer=BufferWithNewResults}}.


%% Legacy, unsorted 2i, should remove?
process_results({error, Reason}, _State) ->
    {error, Reason};
process_results({From, Bucket, Results},
                StateData=#state{from={raw, ReqId, ClientPid}}) ->
    process_query_results(Bucket, Results, ReqId, ClientPid),
    riak_kv_vnode:ack_keys(From), % tell that vnode we're ready for more
    {ok, StateData};
process_results({Bucket, Results},
                StateData=#state{from={raw, ReqId, ClientPid}}) ->
    process_query_results(Bucket, Results, ReqId, ClientPid),
    {ok, StateData};
process_results(done, StateData) ->
    {done, StateData}.

finish({error, Error},
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    %% Notify the requesting client that an error
    %% occurred or the timeout has elapsed.
    ClientPid ! {ReqId, {error, Error}},
    {stop, normal, StateData};
finish(clean,
       StateData=#state{from={raw, ReqId, ClientPid}, merge_sort_buffer=undefined}) ->
    ClientPid ! {ReqId, done},
    {stop, normal, StateData};
finish(clean,
       State=#state{from={raw, ReqId, ClientPid},
                    merge_sort_buffer=MergeSortBuffer,
                    results_sent=ResultsSent,
                    max_results=MaxResults}) ->
    LastResults = sms:done(MergeSortBuffer),
    DownTheWire = case (ResultsSent + length(LastResults)) > MaxResults of
                      true ->
                          lists:sublist(LastResults, MaxResults - ResultsSent);
                      false ->
                          LastResults
                  end,
    ClientPid ! {ReqId, {results, DownTheWire}},
    ClientPid ! {ReqId, done},
    {stop, normal, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

process_query_results(_Bucket, Results, ReqId, ClientPid) ->
    ClientPid ! {ReqId, {results, Results}}.