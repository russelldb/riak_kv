%% -------------------------------------------------------------------
%%
%% riak_get_fsm: coordination of Riak GET requests
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_get_fsm).
-behaviour(gen_fsm).

-import(riak_kv_fsm_common, [get_option/2, get_option/3,
                             get_bucket_props/1,
                             get_n_val/2,
                             get_preflist/4,
                             partition_local_remote/1]).

-include_lib("riak_kv_vnode.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_link/7, test_link/5]).
-endif.
-export([start/6, start_link/6, start/4, start_link/4]).
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([prepare/2,
            validate/2,
            execute/2,
            waiting_vnode_r/2,
            waiting_read_repair/2]).

-type detail() :: timing |
                  vnodes.
-type details() :: [detail()].

-type option() :: {r, pos_integer()} |         %% Minimum number of successful responses
                  {pr, non_neg_integer()} |    %% Minimum number of primary vnodes participating
                  {basic_quorum, boolean()} |  %% Whether to use basic quorum (return early
                  %% in some failure cases.
                  {notfound_ok, boolean()}  |  %% Count notfound reponses as successful.
                  {force_aae, boolean()}    |  %% Force there to be be an AAE exchange for the
                  %% preflist after the GEt has been completed 
                  {timeout, pos_integer() | infinity} | %% Timeout for vnode responses
                  {details, details()} |       %% Return extra details as a 3rd element
                  {details, true} |
                  details |
                  {sloppy_quorum, boolean()} | %% default = true
                  {n_val, pos_integer()} |     %% default = bucket props
                  {crdt_op, true | undefined} | %% default = undefined
                  {request_strategy, request_strategy()}. %% default = get_head

-type options() :: [option()].
-type req_id() :: non_neg_integer().
-type preflist_entry() :: {Idx::non_neg_integer(), node()}.
-type request_strategy() :: get_then_head_plhead | get_then_head_softlimt | head_then_get.

-export_type([options/0, option/0]).

-record(state, {from :: {raw, req_id(), pid()},
                options=[] :: options(),
                n :: pos_integer(),
                preflist2 :: riak_core_apl:preflist_ann(),
                req_id :: non_neg_integer(),
                starttime :: pos_integer(),
                get_core :: riak_kv_get_core:getcore(),
                timeout :: infinity | pos_integer(),
                tref    :: reference(),
                bkey :: {riak_object:bucket(), riak_object:key()},
                bucket_props,
                startnow :: {non_neg_integer(), non_neg_integer(), non_neg_integer()},
                get_usecs :: non_neg_integer(),
                trace = false :: boolean(),
                tracked_bucket=false :: boolean(), %% is per bucket stats enabled for this bucket
                timing = [] :: [{atom(), erlang:timestamp()}],
                calculated_timings :: {ResponseUSecs::non_neg_integer(),
                                       [{StateName::atom(), TimeUSecs::non_neg_integer()}]} | undefined,
                crdt_op :: undefined | true,
                request_type :: undefined | head | get | update,
                force_aae = false :: boolean(),
                override_nodes = [] :: list(),
                %% do it, unless we're told not to @TODO(rdb) make
                %% this a bucket level config, for now/WIP/discovery,
                %% per-request is good enough
                request_strategy = get_then_head_softlimt :: request_strategy(),
                get_pl_entry :: preflist_entry()
               }).

-include("riak_kv_dtrace.hrl").

-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_R, default).
-define(DEFAULT_PR, 0).
-define(DEFAULT_RT, head).
-define(DEFAULT_REQUEST_STRATEGY, get_then_head_softlimt).

%% ===================================================================
%% Public API
%% ===================================================================

%% In place only for backwards compatibility
start(ReqId,Bucket,Key,R,Timeout,From) ->
    start({raw, ReqId, From}, Bucket, Key, [{r, R}, {timeout, Timeout}]).

start_link(ReqId,Bucket,Key,R,Timeout,From) ->
    start({raw, ReqId, From}, Bucket, Key, [{r, R}, {timeout, Timeout}]).

%% @doc Start the get FSM - retrieve Bucket/Key with the options provided
%%
%% {r, pos_integer()}        - Minimum number of successful responses
%% {pr, non_neg_integer()}   - Minimum number of primary vnodes participating
%% {basic_quorum, boolean()} - Whether to use basic quorum (return early
%%                             in some failure cases.
%% {notfound_ok, boolean()}  - Count notfound reponses as successful.
%% {timeout, pos_integer() | infinity} -  Timeout for vnode responses
-spec start({raw, req_id(), pid()}, binary(), binary(), options()) -> {ok, pid()} | {error, any()}.
start(From, Bucket, Key, GetOptions) ->
    Args = [From, Bucket, Key, GetOptions],
    case sidejob_supervisor:start_child(riak_kv_get_fsm_sj,
                                        gen_fsm, start_link,
                                        [?MODULE, Args, []]) of
        {error, overload} ->
            riak_kv_util:overload_reply(From),
            {error, overload};
        {ok, Pid} ->
            {ok, Pid}
    end.

%% Included for backward compatibility, in case someone is, say, passing around
%% a riak_client instace between nodes during a rolling upgrade. The old
%% `start_link' function has been renamed `start' since it doesn't actually link
%% to the caller.
start_link(From, Bucket, Key, GetOptions) -> start(From, Bucket, Key, GetOptions).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).
%% Create a get FSM for testing.  StateProps must include
%% starttime - start time in gregorian seconds
%% n - N-value for request (is grabbed from bucket props in prepare)
%% bucket_props - bucket properties
%% preflist2 - [{{Idx,Node},primary|fallback}] preference list
%%
test_link(ReqId,Bucket,Key,R,Timeout,From,StateProps) ->
    test_link({raw, ReqId, From}, Bucket, Key, [{r, R}, {timeout, Timeout}], StateProps).

test_link(From, Bucket, Key, GetOptions, StateProps) ->
    gen_fsm:start_link(?MODULE, {test, [From, Bucket, Key, GetOptions], StateProps}, []).

-endif.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([From, Bucket, Key, Options0]) ->
    StartNow = os:timestamp(),
    Options = proplists:unfold(Options0),
    StateData = #state{from = From,
                       options = Options,
                       bkey = {Bucket, Key},
                       timing = riak_kv_fsm_timing:add_timing(prepare, []),
                       startnow = StartNow},
    Trace = app_helper:get_env(riak_kv, fsm_trace_enabled),
    case Trace of
        true ->
            riak_core_dtrace:put_tag([Bucket, $,, Key]),
            ?DTRACE(?C_GET_FSM_INIT, [], ["init"]);
        _ ->
            ok
    end,
    {ok, prepare, StateData, 0};
init({test, Args, StateProps}) ->
    %% Call normal init
    {ok, prepare, StateData, 0} = init(Args),

    %% Then tweak the state record with entries provided by StateProps
    Fields = record_info(fields, state),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    F = fun({Field, Value}, State0) ->
                Pos = get_option(Field, FieldPos),
                setelement(Pos, State0, Value)
        end,
    TestStateData = lists:foldl(F, StateData, StateProps),
    {ok, validate, TestStateData, 0}.

%% @private
prepare(timeout, StateData=#state{bkey=BKey={Bucket,_Key},
                                  options=Options,
                                  trace=Trace}) ->
    ?DTRACE(Trace, ?C_GET_FSM_PREPARE, [], ["prepare"]),

    BucketProps = get_bucket_props(Bucket),
    N = get_n_val(Options, BucketProps),
    CrdtOp = get_option(crdt_op, Options),
    ForceAAE = get_option(force_aae, Options, false),
    StatTracked = get_option(stat_tracked, BucketProps, false),
    %% Request strategy
    RequestStrategy = get_option(request_strategy, Options, ?DEFAULT_REQUEST_STRATEGY),
    case N of
        {error, _} = Error ->
            StateData2 = client_reply(Error, StateData),
            {stop, normal, StateData2};
        _ ->
            Preflist = get_preflist(N, BKey, BucketProps, Options),
            GetPlEntry = select_get_entry(Preflist, RequestStrategy),

            Res = new_state_timeout(validate,
				    StateData#state{
				      starttime=riak_core_util:moment(),
				      n = N,
				      bucket_props=BucketProps,
				      preflist2 = Preflist,
				      tracked_bucket = StatTracked,
				      crdt_op = CrdtOp,
				      force_aae = ForceAAE,
				      request_strategy = RequestStrategy,
				      get_pl_entry = GetPlEntry
				     }),
	    Res
    end.

%% @private
validate(timeout, StateData=#state{from = {raw, ReqId, _Pid}, options = Options,
                                   n = N, bucket_props = BucketProps, preflist2 = PL2,
                                   trace=Trace}) ->
    ?DTRACE(Trace, ?C_GET_FSM_VALIDATE, [], ["validate"]),
    AppEnvTimeout = app_helper:get_env(riak_kv, timeout),
    Timeout = case AppEnvTimeout of
                  undefined -> get_option(timeout, Options, ?DEFAULT_TIMEOUT);
                  _ -> AppEnvTimeout
              end,
    R0 = get_option(r, Options, ?DEFAULT_R),
    PR0 = get_option(pr, Options, ?DEFAULT_PR),
    R = riak_kv_util:expand_rw_value(r, R0, BucketProps, N),
    PR = riak_kv_util:expand_rw_value(pr, PR0, BucketProps, N),
    NumVnodes = length(PL2),
    NumPrimaries = length([x || {_,primary} <- PL2]),
    IdxType = [{Part, Type} || {{Part, _Node}, Type} <- PL2],

    case validate_quorum(R, R0, N, PR, PR0, NumPrimaries, NumVnodes) of
        ok ->
            BQ0 = get_option(basic_quorum, Options, default),
            FailR = erlang:max(R, PR), %% fail fast
            FailThreshold =
                case riak_kv_util:expand_value(basic_quorum, BQ0, BucketProps) of
                    true ->
                        erlang:min((N div 2)+1, % basic quorum, or
                                   (N-FailR+1)); % cannot ever get R 'ok' replies
                    _ElseFalse ->
                        N - FailR + 1 % cannot ever get R 'ok' replies
                end,
            AllowMult = get_option(allow_mult, BucketProps),
            NFOk0 = get_option(notfound_ok, Options, default),
            NotFoundOk = riak_kv_util:expand_value(notfound_ok, NFOk0, BucketProps),
            DeletedVClock = get_option(deletedvclock, Options, false),
            GetCore = riak_kv_get_core:init(N, R, PR, FailThreshold,
                                            NotFoundOk, AllowMult,
                                            DeletedVClock, IdxType),

            new_state_timeout(execute, StateData#state{get_core = GetCore,
                                                       timeout = Timeout,
                                                       req_id = ReqId});
        Error ->
            StateData2 = client_reply(Error, StateData),
            {stop, normal, StateData2}
    end.

%% @private validate the quorum values
%% {error, Message} or ok
validate_quorum(R, ROpt, _N, _PR, _PROpt, _NumPrimaries, _NumVnodes) when R =:= error ->
    {error, {r_val_violation, ROpt}};
validate_quorum(R, _ROpt, N, _PR, _PROpt, _NumPrimaries, _NumVnodes) when R > N ->
    {error, {n_val_violation, N}};
validate_quorum(_R, _ROpt, _N, PR, PROpt, _NumPrimaries, _NumVnodes) when PR =:= error ->
    {error, {pr_val_violation, PROpt}};
validate_quorum(_R, _ROpt,  N, PR, _PROpt, _NumPrimaries, _NumVnodes) when PR > N ->
    {error, {n_val_violation, N}};
validate_quorum(_R, _ROpt, _N, PR, _PROpt, NumPrimaries, _NumVnodes) when PR > NumPrimaries ->
    {error, {pr_val_unsatisfied, PR, NumPrimaries}};
validate_quorum(R, _ROpt, _N, _PR, _PROpt, _NumPrimaries, NumVnodes) when R > NumVnodes ->
    {error, {insufficient_vnodes, NumVnodes, need, R}};
validate_quorum(_R, _ROpt, _N, _PR, _PROpt, _NumPrimaries, _NumVnodes) ->
    ok.

%% @private
execute(timeout, StateData0=#state{timeout=Timeout,req_id=ReqId,
                                   bkey=BKey, trace=Trace,
                                   preflist2 = Preflist2,
                                   get_core = GetCore,
                                   request_type = RequestType,
                                   get_pl_entry = GetPlEntry,
                                   override_nodes = OverNodes}) ->
    %% filter the preflist of the GET entry

    Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2,
               IndexNode /= GetPlEntry],
    TRef = schedule_timeout(Timeout),
    case Trace of
        true ->
            ?DTRACE(?C_GET_FSM_EXECUTE, [], ["execute"]),
            Ps = preflist_for_tracing(Preflist),
            ?DTRACE(?C_GET_FSM_PREFLIST, [], Ps);
        _ ->
            ok
    end,
    RequestType2 =
        case RequestType of
            undefined ->
                ?DEFAULT_RT;
            _ ->
                RequestType
        end,
    StateData =
        case RequestType2 of
            head ->
                % Mark the get_core as head_merge so that when determining the
                % response in riak_get_core the specific head_merge function
                % will be used
                %
                % Maybe get from one entry, and send head requests to
                % all the rest of the Preflist

                case GetPlEntry of
                    undefined ->
                        riak_kv_vnode:head(Preflist, BKey, ReqId);
                    Entry ->
                        %% Perform a GET
                        riak_kv_vnode:get([Entry], BKey, ReqId),
                        riak_kv_vnode:head(Preflist, BKey, ReqId)
                end,
                HO_GetCore = riak_kv_get_core:head_merge(GetCore),
                StateData0#state{tref=TRef, get_core = HO_GetCore};
            update ->
                % Need to send get requests, but still merge using head_merge
                % as there will still be head results in the result list, and
                % more head results  may arrive from previous HEAD request
                FetchList = lists:map(fun(Idx) ->
                                            lists:keyfind(Idx, 1, Preflist)
                                        end,
                                        OverNodes),
                riak_kv_vnode:get(FetchList, BKey, ReqId),
                HO_GetCore = riak_kv_get_core:head_merge(GetCore),
                StateData0#state{tref=TRef, get_core = HO_GetCore};
            get ->
                % Only used if the default is switched back to start with a GET
                % not a HEAD
                riak_kv_vnode:get(Preflist, BKey, ReqId),
                StateData0#state{tref=TRef}
        end,
    new_state(waiting_vnode_r, StateData).

%% @private calculate a concatenated preflist for tracing macro
preflist_for_tracing(Preflist) ->
    %% TODO: We can see entire preflist (more than 4 nodes) if we concatenate
    %%       all info into a single string.
    [if is_atom(Nd) ->
             [atom_to_list(Nd), $,, integer_to_list(Idx)];
        true ->
             <<>>                          % eunit test
     end || {Idx, Nd} <- lists:sublist(Preflist, 4)].

%% @private
waiting_vnode_r({r, VnodeResult, Idx, _ReqId},
                    StateData = #state{get_core = GetCore, trace = Trace}) ->
    case Trace of
        true ->
            ShortCode = riak_kv_get_core:result_shortcode(VnodeResult),
            IdxStr = integer_to_list(Idx),
            ?DTRACE(?C_GET_FSM_WAITING_R,
                        [ShortCode],
                        ["waiting_vnode_r", IdxStr]);
        _ ->
            ok
    end,
    % If the query has been to override_nodes will want to replace the result
    % in the result list, not just append to the result list.  The r counter
    % needs updating, regardless if primary, as in override_nodes loop we're
    % no longer bothered as quorum has been met in a previous loop
    UpdGetCore =
        case StateData#state.request_type of
            update ->
                riak_kv_get_core:update_result(Idx,
                                                VnodeResult,
                                                StateData#state.override_nodes,
                                                GetCore);
            _ ->
                riak_kv_get_core:add_result(Idx, VnodeResult, GetCore)
        end,
    case riak_kv_get_core:enough(UpdGetCore) of
        true ->
            % response(GetCore) will either call merge or head_merge. This
            % will depend on the getcore object being set to head_merge or not.
            % head_merge is used for updates or heads.
            %
            % If it is not a get, and head merge is called a fetch return may
            % be made which is a request to update certain objects in the
            % results with bodies (i.e. by substituting a HEAD request with a
            % GET request for that vnode)
            case {StateData#state.request_type,
                    riak_kv_get_core:response(UpdGetCore)} of
                {R, {{fetch, IdxList}, _}} when R /= get ->
                    % Trigger genuine GETs to each vnode index required to
                    % get a merged view of an object.  Hopefully should be
                    % just one
                    NewGC = riak_kv_get_core:update_init(length(IdxList),
                                                            UpdGetCore),
                    execute(timeout,
                                StateData#state{request_type = update,
                                                override_nodes = IdxList,
                                                get_core = NewGC});
                {_, {Reply, UpdGetCore2}} ->
                    StateWithReply = StateData#state{get_core = UpdGetCore2},
                    NewStateData = client_reply(Reply, StateWithReply),
                    update_stats(Reply, NewStateData),
                    maybe_finalize(NewStateData)
            end;
        false ->
            %% don't use new_state/2 since we do timing per state, not per
            %% message in state
            {next_state,
                waiting_vnode_r,
                StateData#state{get_core = UpdGetCore}}
    end;
waiting_vnode_r(request_timeout, StateData = #state{trace=Trace}) ->
    ?DTRACE(Trace, ?C_GET_FSM_WAITING_R_TIMEOUT, [-2],
            ["waiting_vnode_r", "timeout"]),
    S2 = client_reply({error,timeout}, StateData),
    update_stats(timeout, S2),
    finalize(S2).

%% @private
waiting_read_repair({r, VnodeResult, Idx, _ReqId},
                    StateData = #state{get_core = GetCore, trace=Trace}) ->
    case Trace of
        true ->
            ShortCode = riak_kv_get_core:result_shortcode(VnodeResult),
            IdxStr = integer_to_list(Idx),
            ?DTRACE(?C_GET_FSM_WAITING_RR, [ShortCode],
                    ["waiting_read_repair", IdxStr]);
        _ ->
            ok
    end,
    UpdGetCore = riak_kv_get_core:add_result(Idx, VnodeResult, GetCore),
    maybe_finalize(StateData#state{get_core = UpdGetCore});
waiting_read_repair(request_timeout, StateData = #state{trace=Trace}) ->
    ?DTRACE(Trace, ?C_GET_FSM_WAITING_RR_TIMEOUT, [-2],
            ["waiting_read_repair", "timeout"]),
    finalize(StateData).

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info({mbox, _}, StateName, StateData) ->
    %% Delayed mailbox size check response, ignore it
    case timeout_state(StateName) of
	true ->
	    {next_state, StateName, StateData, 0};
	false ->
	    {next_state, StateName, StateData}
    end;
%% @private
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private the `prepare' state sends some `soft-limit' messages to
%% vnode proxies. It only waits for the first good response. The rest
%% of the responses will be handled by `handle_info'. Some states (see
%% function body) are transitioned to via a `timeout' event. This
%% event is set up by the return from the previous state. The docs for
%% gen_fsm say: "If an integer timeout value is provided, a timeout
%% will occur unless an event or a message is received within Timeout
%% milliseconds". In cases where the message is on the mailbox before
%% a state finishes, the Timeout is cancelled before it begins, and
%% handle_info is called, unless handle_info returns a timeout the fsm
%% can just hang in the new state. This function decides if
%% `StateName' is such a state that needs a 0 timeout adding to the
%% return from handle_info.
timeout_state(StateName) ->
    lists:member(StateName, [validate, execute]).


%% Move to the new state, marking the time it started
new_state(StateName, StateData=#state{trace = true}) ->
    {next_state, StateName, add_timing(StateName, StateData)};
new_state(StateName, StateData) ->
    {next_state, StateName, StateData}.

%% Move to the new state, marking the time it started and trigger an immediate
%% timeout.
new_state_timeout(StateName, StateData=#state{trace = true}) ->
    {next_state, StateName, add_timing(StateName, StateData), 0};
new_state_timeout(StateName, StateData) ->
    {next_state, StateName, StateData, 0}.

maybe_finalize(StateData=#state{get_core = GetCore}) ->
    case riak_kv_get_core:has_all_results(GetCore) of
        true -> finalize(StateData);
        false -> {next_state,waiting_read_repair,StateData}
    end.

finalize(StateData=#state{get_core = GetCore, trace = Trace, req_id = ReqID,
                            preflist2 = PL, bkey = {B, K}, 
                            force_aae = ForceAAE}) ->
    {Action, UpdGetCore} = riak_kv_get_core:final_action(GetCore),
    UpdStateData = StateData#state{get_core = UpdGetCore},
    case Action of
        delete ->
            maybe_delete(UpdStateData);
        {read_repair, Indices, RepairObj} ->
            maybe_read_repair(Indices, RepairObj, UpdStateData);
        _Nop ->
            ?DTRACE(Trace, ?C_GET_FSM_FINALIZE, [], ["finalize"]),
            ok
    end,
    case ForceAAE of 
        true ->
            Primaries = 
                [{I, Node} || {{I, Node}, primary} <- PL],
            case length(Primaries) of 
                L when L < 2 ->
                    lager:info("Insufficient Primaries to support force AAE request", []),
                    ok;
                L ->
                    BlueP = lists:nth((ReqID rem L) + 1, Primaries),
                    PinkP = lists:nth(((ReqID + 1) rem L) + 1, Primaries),
                    IndexN = riak_kv_util:get_index_n({B, K}),
                    BlueList = [{riak_kv_vnode:aae_send(BlueP), [IndexN]}], 
                    PinkList = [{riak_kv_vnode:aae_send(PinkP), [IndexN]}], 
                    aae_exchange:start(BlueList, 
                                        PinkList, 
                                        prompt_readrepair([BlueP, PinkP]), 
                                        fun reply_fun/1)
            end;
        false ->
            ok
    end,
    {stop,normal,StateData}.


prompt_readrepair(VnodeList) ->
    C = riak_client:new(local, undefined),
    ElementFun = 
        fun({{B, K}, {_BlueClock, _PinkClock}}) ->
            riak_client:get(B, K, C),
            riak_kv_vnode:rehash(VnodeList, B, K)
        end,
    fun(RepairList) ->
        lists:foreach(ElementFun, RepairList)
    end.

reply_fun({EndStateName, DeltaCount}) ->
    lager:info("AAE Reached end_state=~w with delta_count=~w", 
                [EndStateName, DeltaCount]).


%% Maybe issue deletes if all primary nodes are available.
%% Get core will only requestion deletion if all vnodes
%% replies with the same value.
maybe_delete(StateData=#state{n = N, preflist2=Sent, trace=Trace,
                              req_id=ReqId, bkey=BKey}) ->
    %% Check sent to a perfect preflist and we can delete
    IdealNodes = [{I, Node} || {{I, Node}, primary} <- Sent],
    NotCustomN = not using_custom_n_val(StateData),
    case NotCustomN andalso (length(IdealNodes) == N) of
        true ->
            ?DTRACE(Trace, ?C_GET_FSM_MAYBE_DELETE, [1],
                    ["maybe_delete", "triggered"]),
            riak_kv_vnode:del(IdealNodes, BKey, ReqId);
        _ ->
            ?DTRACE(Trace, ?C_GET_FSM_MAYBE_DELETE, [0],
                    ["maybe_delete", "nop"]),
            nop
    end.

using_custom_n_val(#state{n=N, bucket_props=BucketProps}) ->
    case lists:keyfind(n_val, 1, BucketProps) of
        {_, N} ->
            false;
        _ ->
            true
    end.

%% based on what the get_put_monitor stats say, and a random roll, potentially
%% skip read-repriar
%% On a very busy system with many writes and many reads, it is possible to
%% get overloaded by read-repairs. By occasionally skipping read_repair we
%% can keep the load more managable; ie the only load on the system becomes
%% the gets, puts, etc.
maybe_read_repair(Indices, RepairObj, UpdStateData) ->
    HardCap = app_helper:get_env(riak_kv, read_repair_max),
    SoftCap = app_helper:get_env(riak_kv, read_repair_soft, HardCap),
    Dorr = determine_do_read_repair(SoftCap, HardCap),
    if
        Dorr ->
            read_repair(Indices, RepairObj, UpdStateData);
        true ->
            ok = riak_kv_stat:update(skipped_read_repairs),
            skipping
    end.

determine_do_read_repair(_SoftCap, HardCap) when HardCap == undefined ->
    true;
determine_do_read_repair(SoftCap, HardCap) ->
    Actual = riak_kv_util:gets_active(),
    determine_do_read_repair(SoftCap, HardCap, Actual).

determine_do_read_repair(undefined, HardCap, Actual) ->
    determine_do_read_repair(HardCap, HardCap, Actual);
determine_do_read_repair(_SoftCap, HardCap, Actual) when HardCap =< Actual ->
    false;
determine_do_read_repair(SoftCap, _HardCap, Actual) when Actual =< SoftCap ->
    true;
determine_do_read_repair(SoftCap, HardCap, Actual) ->
    Roll = roll_d100(),
    determine_do_read_repair(SoftCap, HardCap, Actual, Roll).

determine_do_read_repair(SoftCap, HardCap, Actual, Roll) ->
    AdjustedActual = Actual - SoftCap,
    AdjustedHard = HardCap - SoftCap,
    Threshold = AdjustedActual / AdjustedHard * 100,
    Threshold =< Roll.

-ifdef(TEST).
roll_d100() ->
    fsm_eqc_util:get_fake_rng(get_fsm_eqc).
-else.
% technically not a d100 as it has a 0
roll_d100() ->
    crypto:rand_uniform(0, 100).
-endif.

%% Issue read repairs for any vnodes that are out of date
read_repair(Indices, RepairObj,
            #state{req_id = ReqId, starttime = StartTime,
                   preflist2 = Sent, bkey = BKey, crdt_op = CrdtOp,
                   bucket_props = BucketProps, trace = Trace}) ->
    RepairPreflist = [{Idx, Node} || {{Idx, Node}, _Type} <- Sent,
                                     get_option(Idx, Indices) /= undefined],
    case Trace of
        true ->
            Ps = preflist_for_tracing(RepairPreflist),
            ?DTRACE(?C_GET_FSM_RR, [], Ps);
        _ ->
            ok
    end,
    riak_kv_vnode:readrepair(RepairPreflist, BKey, RepairObj, ReqId,
                             StartTime, [{returnbody, false},
                                         {bucket_props, BucketProps},
                                         {crdt_op, CrdtOp}]),
    ok = riak_kv_stat:update({read_repairs, Indices, Sent}).

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

client_reply(Reply, StateData = #state{from = {raw, ReqId, Pid},
                                       options = Options,
                                       timing = Timing,
                                       trace = Trace}) ->
    NewTiming = riak_kv_fsm_timing:add_timing(reply, Timing),
    Msg = case get_option(details, Options, false) of
              false ->
                  {ReqId, Reply};
              [] ->
                  {ReqId, Reply};
              Details ->
                  {OkError, ObjReason} = Reply,
                  Info = client_info(Details,
                                     StateData#state{timing = NewTiming},
                                     []),
                  {ReqId, {OkError, ObjReason, Info}}
          end,
    Pid ! Msg,

    %% calculate timings here, since the trace macro needs total
    %% response time. Stuff the result in state so we don't
    %% need to calculate it again
    {ResponseUSecs, Stages} =
        riak_kv_fsm_timing:calc_timing(NewTiming),
    case Trace of
        true ->
            ShortCode = riak_kv_get_core:result_shortcode(Reply),
            ?DTRACE(?C_GET_FSM_CLIENT_REPLY,
                    [ShortCode, ResponseUSecs], ["client_reply"]);
        _ ->
            ok
    end,
    StateData#state{calculated_timings={ResponseUSecs, Stages},
                    timing = NewTiming}.

update_stats({ok, Obj}, #state{options=Options,
                               tracked_bucket = StatTracked,
                               calculated_timings={ResponseUSecs, Stages}}) ->
    %% Stat the number of siblings and the object size, and timings
    CRDTMod = get_option(crdt_op, Options),
    NumSiblings = riak_object:value_count(Obj),
    ObjFmt = riak_core_capability:get({riak_kv, object_format}, v0),
    ObjSize = riak_object:approximate_size(ObjFmt, Obj),
    Bucket = riak_object:bucket(Obj),
    ok = riak_kv_stat:update({get_fsm, Bucket, ResponseUSecs, Stages,
                              NumSiblings, ObjSize, StatTracked, CRDTMod});
update_stats(_, #state{ bkey = {Bucket, _},
                        options = Options,
                        tracked_bucket = StatTracked,
                        calculated_timings={ResponseUSecs, Stages}}) ->
    CRDTMod = get_option(crdt_op, Options),
    ok = riak_kv_stat:update({get_fsm, Bucket, ResponseUSecs, Stages,
                              undefined, undefined, StatTracked, CRDTMod}).

client_info(true, StateData, Acc) ->
    client_info(details(), StateData, Acc);
client_info([], _StateData, Acc) ->
    Acc;
client_info([timing | Rest], StateData = #state{timing=Timing}, Acc) ->
    {ResponseUsecs, Stages} = riak_kv_fsm_timing:calc_timing(Timing),
    client_info(Rest, StateData, [{response_usecs, ResponseUsecs},
                                  {stages, Stages} | Acc]);
client_info([vnodes | Rest], StateData = #state{get_core = GetCore}, Acc) ->
    Info = riak_kv_get_core:info(GetCore),
    client_info(Rest, StateData, Info ++ Acc);
client_info([Unknown | Rest], StateData, Acc) ->
    client_info(Rest, StateData, [{Unknown, unknown_detail} | Acc]).

%% Add timing information to the state
add_timing(Stage, State = #state{timing = Timing}) ->
    State#state{timing = riak_kv_fsm_timing:add_timing(Stage, Timing)}.

details() ->
    [timing,
     vnodes].

%% @private selects a vnode for the get, depending on mailbox length,
%% etc.
-spec select_get_entry(riak_core_apl:preflist_ann(), request_strategy()) ->
                              undefined | preflist_entry().
select_get_entry(Preflist, get_then_head_softlimt) ->
    %% Pick the least loaded (ideally local) vnode to perform a
    %% GET. The other entries will be used for HEADs.

    {LocalPreflist, RemotePreflist} = partition_local_remote(Preflist),

    case riak_kv_fsm_common:check_mailboxes(LocalPreflist) of
        {true, Entry} ->
            Entry;
        {false, LocalMBoxData} ->
            case riak_kv_fsm_common:check_mailboxes(RemotePreflist) of
                {true, Remote} ->
                     Remote;
                {false, RemoteMBoxData} ->
                    {_Loc, Entry} = riak_kv_fsm_common:select_least_loaded_entry(LocalMBoxData, RemoteMBoxData),
		    Entry
            end
    end;
select_get_entry(Preflist, get_then_head_plhead) ->
    %% always choose the head of the preflist, for maximum disk cache
    %% use (but possible worse behaviour if the head node is
    %% anavailable?) The chosen entry will be used to do a GET, while
    %% the other entries will be used for HEAD requests
    {Entry, _Type} = hd(Preflist),
    Entry;
select_get_entry(_Preflist, head_then_get) ->
    %% no preselection of a vnode to GET from, do N heads, then pick a
    %% GET vnode (see `execute')
    undefined.

-ifdef(TEST).
-define(expect_msg(Exp,Timeout),
        ?assertEqual(Exp, receive Exp -> Exp after Timeout -> timeout end)).

%% SLF: Comment these test cases because of OTP app dependency
%%      changes: riak_kv_vnode:test_vnode/1 now relies on riak_core to
%%      be running ... eventually there's a call to
%%      riak_core_ring_manager:get_raw_ring().

determine_do_read_repair_test_() ->
    [
        {"soft cap is undefined, actual below", ?_assert(determine_do_read_repair(undefined, 7, 5))},
        {"soft cap is undefined, actual above", ?_assertNot(determine_do_read_repair(undefined, 7, 10))},
        {"soft cap is undefined, actual at", ?_assertNot(determine_do_read_repair(undefined, 7, 7))},
        {"hard cap is undefiend", ?_assert(determine_do_read_repair(3000, undefined))},
        {"actual below soft cap", ?_assert(determine_do_read_repair(3000, 7000, 2000))},
        {"actual equals soft cap", ?_assert(determine_do_read_repair(3000, 7000, 3000))},
        {"actual above hard cap", ?_assertNot(determine_do_read_repair(3000, 7000, 9000))},
        {"actaul equals hard cap", ?_assertNot(determine_do_read_repair(3000, 7000, 7000))},
        {"hard cap == soft cap, actual below", ?_assert(determine_do_read_repair(100, 100, 50))},
        {"hard cap == soft cap, actual above", ?_assertNot(determine_do_read_repair(100, 100, 150))},
        {"hard cap == soft cap, actual equals", ?_assertNot(determine_do_read_repair(100, 100, 100))},
        {"roll below threshold", ?_assertNot(determine_do_read_repair(5000, 15000, 10000, 1))},
        {"roll exactly threshold", ?_assert(determine_do_read_repair(5000, 15000, 10000, 50))},
        {"roll above threshold", ?_assert(determine_do_read_repair(5000, 15000, 10000, 70))}
    ].

-ifdef(BROKEN_EUNIT_PURITY_VIOLATION).
get_fsm_test_() ->
    {spawn, [{ setup,
               fun setup/0,
               fun cleanup/1,
               [
                fun happy_path_case/0,
                fun n_val_violation_case/0
               ]
             }]}.

setup() ->
    %% Set infinity timeout for the vnode inactivity timer so it does not
    %% try to handoff.
    application:load(riak_core),
    application:set_env(riak_core, vnode_inactivity_timeout, infinity),
    application:load(riak_kv),
    application:set_env(riak_kv, storage_backend, riak_kv_memory_backend),
    application:set_env(riak_core, default_bucket_props, [{r, quorum},
            {w, quorum}, {pr, 0}, {pw, 0}, {rw, quorum}, {n_val, 3},
            {basic_quorum, true}, {notfound_ok, false}]),

    %% Have tracer on hand to grab any traces we want
    riak_core_tracer:start_link(),
    riak_core_tracer:reset(),
    riak_core_tracer:filter([{riak_kv_vnode, readrepair}],
                   fun({trace, _Pid, call,
                        {riak_kv_vnode, readrepair,
                         [Preflist, _BKey, Obj, ReqId, _StartTime, _Options]}}) ->
                           [{rr, Preflist, Obj, ReqId}]
                   end),
    ok.

cleanup(_) ->
    dbg:stop_clear().

happy_path_case() ->
    riak_core_tracer:collect(5000),

    %% Start 3 vnodes
    Indices = [1, 2, 3],
    Preflist2 = [begin
                     {ok, Pid} = riak_kv_vnode:test_vnode(Idx),
                     {{Idx, Pid}, primary}
                 end || Idx <- Indices],
    Preflist = [IdxPid || {IdxPid,_Type} <- Preflist2],

    %% Decide on some parameters
    Bucket = <<"mybucket">>,
    Key = <<"mykey">>,
    Nval = 3,
    BucketProps = bucket_props(Bucket, Nval),

    %% Start the FSM to issue a get and  check notfound

    ReqId1 = 112381838, % erlang:phash2(erlang:now()).
    R = 2,
    Timeout = 1000,
    {ok, _FsmPid1} = test_link(ReqId1, Bucket, Key, R, Timeout, self(),
                               [{starttime, 63465712389},
                               {n, Nval},
                               {bucket_props, BucketProps},
                               {preflist2, Preflist2}]),
    ?assertEqual({error, notfound}, wait_for_reqid(ReqId1, Timeout + 1000)),

    %% Update the first two vnodes with a value
    ReqId2 = 49906465,
    Value = <<"value">>,
    Obj1 = riak_object:new(Bucket, Key, Value),
    riak_kv_vnode:put(lists:sublist(Preflist, 2), {Bucket, Key}, Obj1, ReqId2,
                      63465715958, [{bucket_props, BucketProps}], {raw, ReqId2, self()}),
    ?expect_msg({ReqId2, {w, 1, ReqId2}}, Timeout + 1000),
    ?expect_msg({ReqId2, {w, 2, ReqId2}}, Timeout + 1000),
    ?expect_msg({ReqId2, {dw, 1, ReqId2}}, Timeout + 1000),
    ?expect_msg({ReqId2, {dw, 2, ReqId2}}, Timeout + 1000),

    %% Issue a get, check value returned.
    ReqId3 = 30031523,
    {ok, _FsmPid2} = test_link(ReqId3, Bucket, Key, R, Timeout, self(),
                              [{starttime, 63465712389},
                               {n, Nval},
                               {bucket_props, BucketProps},
                               {preflist2, Preflist2}]),
    ?assertEqual({ok, Obj1}, wait_for_reqid(ReqId3, Timeout + 1000)),

    %% Check readrepair issued to third node
    ExpRRPrefList = lists:sublist(Preflist, 3, 1),
    riak_kv_test_util:wait_for_pid(_FsmPid2),
    riak_core_tracer:stop_collect(),
    ?assertEqual([{0, {rr, ExpRRPrefList, Obj1, ReqId3}}],
                 riak_core_tracer:results()).


n_val_violation_case() ->
    ReqId1 = 13210434, % erlang:phash2(erlang:now()).
    Bucket = <<"mybucket">>,
    Key = <<"badnvalkey">>,
    Nval = 3,
    R = 5,
    Timeout = 1000,
    BucketProps = bucket_props(Bucket, Nval),
    %% Fake three nodes
    Indices = [1, 2, 3],
    Preflist2 = [begin
                     {{Idx, self()}, primary}
                 end || Idx <- Indices],
    {ok, _FsmPid1} = test_link(ReqId1, Bucket, Key, R, Timeout, self(),
                               [{starttime, 63465712389},
                               {n, Nval},
                               {bucket_props, BucketProps},
                               {preflist2, Preflist2}]),
    ?assertEqual({error, {n_val_violation, 3}}, wait_for_reqid(ReqId1, Timeout + 1000)).


wait_for_reqid(ReqId, Timeout) ->
    receive
        {ReqId, Msg} -> Msg
    after Timeout ->
            {error, req_timeout}
    end.

bucket_props(Bucket, Nval) -> % riak_core_bucket:get_bucket(Bucket).
    [{name, Bucket},
     {allow_mult,false},
     {big_vclock,50},
     {chash_keyfun,{riak_core_util,chash_std_keyfun}},
     {dw,quorum},
     {last_write_wins,false},
     {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
     {n_val,Nval},
     {old_vclock,86400},
     {postcommit,[]},
     {precommit,[]},
     {r,quorum},
     {rw,quorum},
     {small_vclock,50},
     {w,quorum},
     {young_vclock,20}].


-endif. % BROKEN_EUNIT_PURITY_VIOLATION
-endif.
