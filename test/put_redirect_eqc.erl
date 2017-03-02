%% @note Remember to compile riak_kv_util like this:
%% ERL_LIBS=deps erlc -I include  +debug_info src/riak_kv_util.erl -o ebin 

-module(put_redirect_eqc).

-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").

-record(state,
        { fsm_pid,
          next_state = not_started,
          n_val,
          nodes,
          bad_coordinators,
          coordinating_node = none,
          fake_fsm_pid,
          coordinator_tref,
          coordinator_has_timed_out = false,
          remote_coordinator_ref,
          remote_coord_start_ref}).


-define(BUCKET_TYPE, <<"my_bucket_type">>).
-define(BUCKET, <<"my_bucket">>).
-define(KEY, <<"da_key">>).
-define(VALUE, 42).


-define(APP_HELPER_CALLOUT(Args), 
        ?CALLOUT(app_helper, get_env, Args,
                 erlang:apply(?MODULE, app_get_env, Args))).

-define(LAGER_ERROR,
        ?CALLOUT(lager, error, [?WILDCARD, ?WILDCARD], ok)).

-define(KV_STAT_CALLOUT,
        ?CALLOUT(riak_kv_stat, update, [?WILDCARD], ok)).

-define(START_STATE(State), 
        ?CALLOUT(riak_kv_put_fsm_comm, start_state, [State], ok)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_redirect() ->
    ?SETUP( fun setup/0,
            ?FORALL(Cmds, 
                    commands(?MODULE),
                    begin
                        start(),
                        {H, S, Res} = run_commands(?MODULE,Cmds),
                        stop(S),
                        % io:format("mock trace: ~p~n", [eqc_mocking:get_trace(api_spec())]),
                        pretty_commands(?MODULE, Cmds, {H, S, Res},
                                        aggregate(call_features(H),
                                                  Res == ok))
                    end)).

setup() ->
    init_mock_module(riak_kv_util, riak_kv_util_mock),
    eqc_mocking:start_mocking(api_spec()),
    fun teardown/0.

init_mock_module(Module, MockedModuleName) ->
    %% testing has failed ensure that the debug_info version of the
    %% module is loaded first
    code:purge(Module),
    code:load_file(Module),
    Api = extract_api_for_mocked_module(MockedModuleName),
    eqc_mock_replace:module(Module, MockedModuleName, Api).

extract_api_for_mocked_module(Module) ->
    #api_spec{modules=Modules} = api_spec(),
    case lists:keyfind(Module, #api_module.name, Modules) of
        false ->
            exit({no_such_mocked_module, Module});
        #api_module{functions=Functions} ->
            convert_api_funs_to_tuple(Functions)
    end.

convert_api_funs_to_tuple(Functions) ->
    lists:map( fun convert_api_fun_to_tuple/1,
                   Functions).

convert_api_fun_to_tuple(#api_fun{name=Name, arity=Arity}) ->
    {Name, Arity}.


teardown() ->
    eqc_mocking:stop_mocking().



start() ->
    ok.

stop(S) ->
    catch exit(S#state.fsm_pid, kill),
    catch exit(S#state.fake_fsm_pid, kill).


initial_state() ->
    #state{fake_fsm_pid = fake_fsm(),
           coordinator_tref = make_ref(),
           remote_coord_start_ref = make_ref(),
           remote_coordinator_ref = make_ref()}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%% Mocking

api_spec() ->
    #api_spec{
       language = erlang,
       modules  = [
                   % We don't care about calls to app_helper and they are static, so
                   % we place them in a separate module.
                   #api_module{
                      name = app_helper, fallback = app_helper_mock,
                      functions = [] },
                   #api_module{
                      name = riak_kv_stat,
                      functions = [ #api_fun{ name = update, arity = 1} ]},
                   #api_module{
                      name = riak_core_bucket,
                      functions = [ #api_fun{ name = get_bucket, arity = 1} ]},
                   #api_module{
                      name = riak_kv_put_fsm_comm,
                      functions = [ #api_fun{ name = start_state, arity = 1},
                                    #api_fun{ name = schedule_request_timeout, arity=1},
                                    #api_fun{ name = start_remote_coordinator, arity=3}
                                  ]},
                   #api_module{
                      name = riak_core_node_watcher, 
                      functions = [ #api_fun{ name = nodes, arity=1}
                                                % returns : 
                                                % -type preflist_ann() :: [{{index(),
                                                % node()}, primary|fallback}].
                                  ]},
                   #api_module{
                      name = riak_core_apl,
                      functions = [ #api_fun{ name = get_apl_ann, arity=3},
                                    #api_fun{ name = get_primary_apl, arity=3}
                                  ]},
                   #api_module{
                      name = riak_core_capability,
                      functions = [ #api_fun{ name = get, arity=2} 
                                  ]},
                   #api_module{
                      name = riak_kv_hooks,
                      functions = [ #api_fun{ name = get_conditional_postcommit, arity=2}
                                  ]},
                   #api_module{
                      name = riak_kv_util_mock,
                      functions = [ #api_fun{ name = get_random_element, arity=1}
                                  ]},
                   #api_module{
                      name = lager,
                      functions = [ #api_fun{ name = error, arity=2 } 
                                  ]}
                  ]}.
                                              


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%% Commands


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_put(From, Object, PutOptions, _Nodes) ->
    {ok, Pid} = riak_kv_put_fsm:start_link(From, Object, PutOptions),
    unlink(Pid),
    Pid.


start_put_args(_S) ->
    [from(), new_object(), put_options(), riak_nodes()].

start_put_pre(S) ->
    S#state.next_state == not_started.


%% @todo: mock riak_kv_get_put_monitor and avoid the parallelism with the
%% KV_STAT_CALLOUT that it causes.
start_put_callouts(_S, _Args) ->
    ?PAR([
          ?KV_STAT_CALLOUT
         ,?CALLOUT(riak_kv_put_fsm_comm, start_state, [prepare], ok)]).


start_put_post(_S, _Args, Pid) ->
    check_pid(Pid).


start_put_next(S, Pid, [_From, _Object, PutOptions, Nodes]) ->
    S#state{fsm_pid = Pid,
            next_state = prepare,
            nodes = Nodes,
            bad_coordinators = proplists:get_value(bad_coordinators, PutOptions),
            n_val = proplists:get_value(n_val, PutOptions)
           }.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_prepare(Pid) ->
    gen_fsm:send_event(Pid, {start, prepare}).


%% Do this in a worker process so that the previous step has collected all callouts
%% before we begin.
%% In a newer EQC/OTP combo it would be safe to use 
%%  with_parameter(default_process, worker, commands(?MODULE))
%% in the ?FORALL.
start_prepare_process(_,_) ->
    worker.

start_prepare_args(S) ->
    [S#state.fsm_pid].

start_prepare_pre(S) ->
    S#state.next_state==prepare.


%% Note: we use the presence of the correct bad_coordinators in the options used to
%% start the remote coordinator to determine if this node is sending enough
%% information to the coordinating node. Otherwise we will see the sibling problem in
%% https://github.com/basho/riak_kv/issues/1188. 
start_prepare_callouts(S, _Args) ->
    io:format("#"),
    {APL, APLChoice, CoordinatingNode} = calc_apl_arguments(S),
%    ?APP_HELPER_CALLOUT([riak_core, default_bucket_props]),
    ?CALLOUT(riak_core_bucket, get_bucket, [?WILDCARD], 
             (app_helper_mock:get_env(riak_core, default_bucket_props))),
    ?CALLOUT(riak_core_node_watcher, nodes, [riak_kv],
             (S#state.nodes)),
    ?CALLOUT(riak_core_apl,get_apl_ann, [?WILDCARD, ?WILDCARD, ?WILDCARD],
                       APL),
    % @todo: take into account if we use
    % sloppy_quorum (true is default)
    case should_node_coordinate(S) of
        true ->
            io:format("*"),
            ?CALLOUT(riak_kv_put_fsm_comm, start_state, [validate], ok);
        false ->
            io:format(">"),
            ?CALLOUT(riak_kv_util_mock, get_random_element, [?WILDCARD], 
                     APLChoice),
            ?CALLOUT(riak_core_capability, get, [?WILDCARD, ?WILDCARD], enabled),
%            ?APP_HELPER_CALLOUT([riak_kv, retry_put_coordinator_failure, true]),
            ?MATCH({Options, _},
                   ?CALLOUT(riak_kv_put_fsm_comm, start_remote_coordinator, 
                            [CoordinatingNode, [?WILDCARD, ?WILDCARD, ?VAR], ?WILDCARD], 
                            {S#state.fake_fsm_pid, S#state.coordinator_tref, S#state.remote_coord_start_ref})),
            ?ASSERT(?MODULE, correct_options, [Options, S], 
                     {bad_coordinators_in_options, Options,
                      should_have_matched, S#state.bad_coordinators}),
            ?KV_STAT_CALLOUT
    end.


correct_options(Options, S) ->
    Expected = lists:usort(S#state.bad_coordinators),
    Sent = lists:usort(proplists:get_value(bad_coordinators, Options, [])),
    Expected == Sent.

calc_apl_arguments(S) ->
    APL = active_preflist(sloppy_quorum, S),
    APLChoice = lists:last(APL),
    {{_, CoordinatingNode}, _} = APLChoice,
    {APL, APLChoice, CoordinatingNode}.


start_prepare_features(S, _Args, _) ->
    case {should_node_coordinate(S), S#state.coordinator_has_timed_out} of
        {true, true} ->
            [coordinate_after_timeout];
        {true, false} ->
            [coordinate];
        {false, true} ->
            [redirect_after_timeout];
        {false, false} ->
            [redirect]
    end.

start_prepare_next(S, _FakeFsmPid, _Args) ->
    case should_node_coordinate(S) of
        true -> 
            io:format("+"),
            S#state{next_state = done}; % should be validate if we want to test more
        false ->
            io:format("-"),
            {_, _, CoordinatingNode} = calc_apl_arguments(S),
            S#state{next_state = waiting_remote_coordinator,
                    coordinating_node = CoordinatingNode}
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
waiting_remote_coordinator(Pid, {executing_ack, Node, RemoteStartRef, RemotePid}) ->
    riak_kv_put_fsm:remote_coordinator_started(Pid, RemoteStartRef, RemotePid),
    Pid ! {ack, Node, now_executing};
waiting_remote_coordinator(Pid, {coordinator_timeout, TRef}) ->
    Pid ! {timeout, TRef, coordinator_timeout};
waiting_remote_coordinator(Pid, {coordinator_not_responsible, RemoteStartRef, RemotePid}) ->
    riak_kv_put_fsm:remote_coordinator_started(Pid, RemoteStartRef, RemotePid),
    Pid ! {'DOWN', make_ref(), process, RemotePid, not_responsible}.

waiting_remote_coordinator_process(_,_) ->
    worker.

waiting_remote_coordinator_args(S) ->
    io:format("W"),
    elements([
              [S#state.fsm_pid, {executing_ack, S#state.coordinating_node, S#state.remote_coord_start_ref, S#state.fake_fsm_pid}],
              [S#state.fsm_pid, {coordinator_not_responsible, S#state.remote_coord_start_ref, S#state.fake_fsm_pid}],
              [S#state.fsm_pid, {coordinator_timeout, S#state.coordinator_tref}]
             ]).

waiting_remote_coordinator_pre(S) ->
    S#state.next_state == waiting_remote_coordinator.

waiting_remote_coordinator_callouts(_S, [_FsmPid, {executing_ack, _, _, _}]) ->
    ?CALLOUT(riak_kv_stat, update, [{fsm_exit, puts}], ok);
waiting_remote_coordinator_callouts(_S, [_FsmPid, {coordinator_not_responsible, _, _}]) ->
    ?CALLOUT(riak_kv_put_fsm_comm, start_state, [prepare], ok);
waiting_remote_coordinator_callouts(_S, [_FsmPid, {coordinator_timeout, _}]) ->
    ?CALLOUT(riak_kv_put_fsm_comm, start_state, [prepare], ok).
                 

waiting_remote_coordinator_features(S, _Args, {ack, _, _}) ->
    case S#state.coordinator_has_timed_out of 
        false -> 
            [remote_executes];
        true ->
            [remote_executes_after_timeout]
    end;
waiting_remote_coordinator_features(_S, _Args, {'DOWN', _, process, _, not_responsible}) ->
    [remote_not_responsible];
waiting_remote_coordinator_features(_S, _Args, {timeout, _, coordinator_timeout}) ->
    [remote_timeout].

waiting_remote_coordinator_next(S, _Res, [_FsmPid, {executing_ack, _, _, _}]) ->
    S#state{next_state = done};
waiting_remote_coordinator_next(S, _Res, [_FsmPid, {coordinator_not_responsible, _, _}]) ->
    S#state{next_state = prepare,
            bad_coordinators = [S#state.coordinating_node | 
                                S#state.bad_coordinators]};
waiting_remote_coordinator_next(S, _Res, [_FsmPid, {coordinator_timeout, _}]) ->
    S#state{next_state = prepare,
            bad_coordinators = [S#state.coordinating_node | 
                                S#state.bad_coordinators],
            coordinator_has_timed_out = true}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_validate(Pid) ->
    gen_fsm:send_event(Pid, {start, validate}).

start_validate_process(_,_) ->
    worker.

start_validate_args(S) ->
    [S#state.fsm_pid].

start_validate_pre(S) ->
    S#state.next_state == validate.

start_validate_callouts(_S, _Args) ->
    ?SEQ([?CALLOUT(riak_kv_hooks, get_conditional_postcommit, [?WILDCARD, ?WILDCARD],
                   [])
         ,?CALLOUT(riak_kv_put_fsm_comm, start_state, [precommit], ok)
         ]).

start_validate_next(S, _, _Args) ->
    S#state{next_state = precommit}.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_precommit(Pid) ->
    gen_fsm:send_event(Pid, {start, precommit}).

start_precommit_process(_, _) ->
    worker.

start_precommit_args(S) ->
    [S#state.fsm_pid].

start_precommit_pre(S) ->
    S#state.next_state == precommit.

start_precommit_callouts(_S, _Args) ->
    request_timeout(finite).


start_precommit_next(S, _, _Args) ->
    S#state{next_state = waiting_local_vnode}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%% Generators

from() ->
    {raw, 1, self()}.

new_object() ->
    riak_object:new({?BUCKET_TYPE, ?BUCKET}, ?KEY, ?VALUE).

put_options() ->
    [{n_val, 3}, 
     {w, quorum}, 
     {chash_keyfun, {riak_core_util, chash_std_keyfun}},
     {sloppy_quorum, true},
     {bad_coordinators, bad_coordinators()}].
       
bad_coordinators() ->
    elements([
              [],
              [node_c]
             ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper functions

riak_nodes() ->
%%    [node_a, node_b, node_c, node()].
    elements([
              [node_a, node_b, node_c, node()],
              [node_a, node_b, node_c, node_d, node()]
             ]).

preflist(Nodes, NVal) ->
    lists:sublist(Nodes, NVal).

is_node_primary(Nodes) ->
    lists:member(node(), lists:sublist(Nodes, 3)).

should_node_coordinate(S) ->
    is_node_primary(S#state.nodes -- S#state.bad_coordinators).

active_preflist(sloppy_quorum, S) ->
    All = [{{0, N}, node_type(N)} || N <- (S#state.nodes -- S#state.bad_coordinators)],
    lists:sublist(All, S#state.n_val).

node_type(node_a) ->
    primary;
node_type(node_b) ->
    primary;
node_type(node_c) ->
    primary;
node_type(_N) ->
    fallback.


request_timeout(infinity) ->
    ?CALLOUT(riak_kv_put_fsm_comm, schedule_request_timeout, [infinity], undefined);
request_timeout(_Timeout) ->
    ?CALLOUT(riak_kv_put_fsm_comm, schedule_request_timeout, [?WILDCARD], reqquest_timeout_ref).

check_pid(Pid) -> 
    is_pid(Pid) andalso erlang:is_process_alive(Pid).


%% need to have a real process around so that the put FSM can kill it when the
%% coordinator_timeout happens.
fake_fsm() ->
    spawn( fun fake_loop/0 ).

fake_loop() ->
    receive
        _M ->
            fake_loop()
    end.
