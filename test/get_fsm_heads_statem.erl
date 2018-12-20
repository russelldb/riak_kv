%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2018, Russell Brown
%%% @doc
%%%  statem that tests the HEADS->UPDATE loop of the get fsm
%%% @end
%%% Created : 13 Dec 2018 by Russell Brown <russell@wombat.me>

-module(get_fsm_heads_statem).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include("../src/riak_kv_wm_raw.hrl").

-compile(export_all).

-define(BUCKET, <<"b">>).
-define(KEY, <<"k">>).
-define(VALUE, <<"v">>).

-define(REQID, 1000).
-define(DEFAULT_BUCKET_PROPS,
        [{allow_mult, true},
         {chash_keyfun, {riak_core_util, chash_std_keyfun}},
         {basic_quorum, true},
         {notfound_ok, false},
         {r, quorum},
         {pr, 0}]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).


%% -- State ------------------------------------------------------------------
-record(state,{
          %% maybe this should be an eqc_fsm, not an eqc_statem??
          state = init :: init | validate | execute | waiting_r,
          fsm_state_history = [] :: list(any()),
          preflist = undefined,
          n = undefined,
          r = undefined,
          %% the list of results to return to the fsm
          results = [] :: list(any())
         }).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% -- Operations -------------------------------------------------------------

%% --- Operation: init ---
%% @doc init_pre/1 - Precondition for generation
-spec init_pre(S :: eqc_statem:symbolic_state()) -> boolean().
init_pre(#state{state=init}) ->
    true;
init_pre(_S) ->
    false.

%% @doc init_args - Argument generator
-spec init_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
init_args(#state{n=N, r=R, preflist=PL}) ->
    [PL, N, R].

%% @doc init_pre/2 - Precondition for init
-spec init_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
init_pre(#state{state=init}, _Args) ->
    true;
init_pre(_S, _Args) ->
    false.

%% @doc init - The actual operation
init(Preflist, N, R) ->
    %% TODO: do I need to start a process for this pid, rather than using eqc's pid?
    From = {raw, ?REQID, self()},

    StateProps = [{n, N},
                  {bucket_props, [{n_val, N} | ?DEFAULT_BUCKET_PROPS]},
                  {preflist2, Preflist},
                  {request_type, head}],

    Args = [From, ?BUCKET, ?KEY, [{r, R}]],
    riak_kv_get_fsm:init({test, Args, StateProps}).

%% @doc init_next - Next state function
-spec init_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
init_next(S=#state{fsm_state_history=Hist}, Value, [PL, N, R]) ->
    S#state{fsm_state_history = [Value | Hist],
            state = validate,
            preflist = PL,
            n = N,
            r = R}.

%% @doc init_post - Postcondition for init
-spec init_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
init_post(_S, _Args, {ok, validate, _FSMState, 0}) ->
    true;
init_post(_S, _A, Other) ->
    {false, Other}.

%% --- Operation: validate ---
%% @doc validate_pre/1 - Precondition for generation
-spec validate_pre(S :: eqc_statem:symbolic_state()) -> boolean().
validate_pre(#state{state=validate}) ->
    true;
validate_pre(_S) ->
    false.

%% @doc validate_args - Argument generator
-spec validate_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
validate_args(#state{fsm_state_history=FSMH}) ->
    [fsm_state_data(FSMH)].

%% @doc validate_pre/2 - Precondition for validate
-spec validate_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
validate_pre(_S, [undefined]) ->
    false;
validate_pre(#state{state=validate}, [_FSMStateData]) ->
    true.

%% @doc validate - The actual operation
validate(FSMStateData) ->
    %% unpack here as the state data should be concrete, not symbolic
    %% now
    {ok, validate, FSMState, 0} = FSMStateData,
    riak_kv_get_fsm:validate(timeout, FSMState).

%% @doc validate_next - Next state function
-spec validate_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
validate_next(S=#state{fsm_state_history=Hist}, Value, [_FSMStateData]) ->
    S#state{fsm_state_history=[Value | Hist], state=execute}.

%% @doc validate_post - Postcondition for validate
-spec validate_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
validate_post(_S, [_FSMStateData], {next_state, execute, _FSMState, 0}) ->
    true;
validate_post(_S, _Args, Res) ->
    {false, Res}.

%% --- Operation: execute ---
%% @doc execute_pre/1 - Precondition for generation
-spec execute_pre(S :: eqc_statem:symbolic_state()) -> boolean().
execute_pre(#state{state=execute}) ->
    true;
execute_pre(_S) ->
    false.

%% @doc execute_args - Argument generator
-spec execute_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
execute_args(#state{fsm_state_history=FSMH}) ->
    [fsm_state_data(FSMH)].

%% @doc execute_pre/2 - Precondition for execute
-spec execute_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
execute_pre(_S, [undefined]) ->
    false;
execute_pre(#state{state=execute}, [_FSMStateData]) ->
    true.

%% @doc execute - The actual operation
execute(FSMStateData) ->
    {next_state, execute, FSMState, 0} = FSMStateData,
    riak_kv_get_fsm:execute(timeout, FSMState).

%% @doc execute_next - Next state function
-spec execute_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
execute_next(S=#state{fsm_state_history=Hist}, Value, [_FSMStateData]) ->
    S#state{fsm_state_history=[Value | Hist], state=waiting_r}.

%% @doc execute_post - Postcondition for execute
-spec execute_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
execute_post(_S, [_FSMStateData], {next_state, waiting_vnode_r, _FSMState}) ->
    true;
execute_post(_S, _Args, Res) ->
    {false, Res}.

%% --- Operation: waiting_r ---
%% @doc waiting_r_pre/1 - Precondition for generation
-spec waiting_r_pre(S :: eqc_statem:symbolic_state()) -> boolean().
waiting_r_pre(#state{state=waiting_r}) ->
    true;
waiting_r_pre(_S) ->
    false.

%% @doc waiting_r_args - Argument generator
-spec waiting_r_args(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen([term()]).
waiting_r_args(#state{fsm_state_history=FSMH, results=Results}) ->
    [fsm_state_data(FSMH), Results].

%% @doc waiting_r_pre/2 - Precondition for waiting_r
-spec waiting_r_pre(S, Args) -> boolean()
    when S    :: eqc_statem:symbolic_state(),
         Args :: [term()].
waiting_r_pre(_S, [undefined, _]) ->
    false;
waiting_r_pre(#state{state=waiting_r}, _Args) ->
    true.

%% @doc waiting_r - The actual operation
waiting_r(FSMStateData, Results) ->
    {new_state, waiting_vnode_r, FSMState} = FSMStateData,
    riak_kv_get_fsm:waiting_vnode_r(NextResult, FSMState).

%% @doc waiting_r_next - Next state function
-spec waiting_r_next(S, Var, Args) -> NewS
    when S    :: eqc_statem:symbolic_state() | eqc_state:dynamic_state(),
         Var  :: eqc_statem:var() | term(),
         Args :: [term()],
         NewS :: eqc_statem:symbolic_state() | eqc_state:dynamic_state().
waiting_r_next(S=#state{fsm_state_history=Hist, results=Results}, Value, [_FSMStateData, _NextResult]) ->
    NewResults = tl(Results),
    S#state{fsm_state_history=[Value | Hist], results=NewResults, state=waiting_r}.

%% @doc waiting_r_post - Postcondition for waiting_r
-spec waiting_r_post(S, Args, Res) -> true | term()
    when S    :: eqc_state:dynamic_state(),
         Args :: [term()],
         Res  :: term().
waiting_r_post(_S, [_FSMStateData], _Res) ->
    true.



%% @doc weight/2 - Distribution of calls
-spec weight(S, Cmd) -> integer()
    when S   :: eqc_statem:symbolic_state(),
         Cmd :: atom().
weight(_S, _Cmd) -> 1.

%% @doc Default generated property
-spec prop_gets_heads() -> eqc:property().
prop_gets_heads() ->
    ?FORALL({SI1, Cmds}, ?LET(SI, gen_initial_state(), {SI, commands(?MODULE, SI)}),
            begin
                #state{n=N, r=R, preflist=_PL} = SI1,
                {H, S, Res} = run_commands(?MODULE,Cmds),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                measure(length, length(Cmds),
                                        aggregate(command_names(Cmds),
                                                  aggregate([{N, R}],
                                                            Res == ok))))
            end).


%% -- generators ---------------------------------------------------------------

%% generate the params before any commands run so we can use the
%% results to set up / verify mocks
gen_initial_state() ->
    ?LET({N2, R1, PL1, Results},
         ?LET({N1, R, PL},
              ?LET(N, gen_n(), {N, gen_r(N), gen_preflist(N)}),
              {N1, R, PL, gen_results(PL)}),
         begin
             S = initial_state(),
             S#state{n = N2,
                     r = R1,
                     preflist = PL1,
                     results = Results}
         end).

%% this is really the meat of the whole sandwich. Generate an ordered
%% set of replies that we're going to feed to the fsm, in order. R
%% heads, then randomly either the result of updates or the result of
%% gets (TODO use mocks to verify get/head etc are called as expected)
%% In this first iteration, just generate N objects / heads
%% @TODO generate not_founds, some errors, and some tombstones too!
gen_results(PL) ->
    ?LET({Clocks, Rands}, {gen_clocks(PL, PL, later, [{undefined, vclock:fresh()}]),
                           vector(length(PL), int())},
         begin
             NodeClocks = [{Node, Clock} || {Node, Clock} <- Clocks,
                                            Node /= undefined],
             %% given how the clocks are created, the accumulator will
             %% have the greatest clocks at the start (and the
             %% earliest at the end) if we use the clocks in this
             %% order as results we don't test as well, so randomise
             %% them
             RandClocks = [{Node, Clock} || {_Order, {Node, Clock}} <- lists:sort(lists:zip(Rands, NodeClocks))],
             [{Node, gen_val(Clock)} || {Node, Clock} <- RandClocks]
         end).

gen_val(Clock) ->
    frequency([{1, tombstone(Clock)}, {6, object_and_head(Clock)}, {1, not_found()}]).

not_found() ->
    {not_found, {not_found, not_found}}.

tombstone(Clock) ->
    Obj0 = riak_object:new(?BUCKET, ?KEY, <<>>, dict:store(?MD_DELETED,
                                                           "true", dict:new())),
    Tombstone = riak_object:set_vclock(Obj0, Clock),
    Head = riak_object:convert_object_to_headonly(?BUCKET, ?KEY, Tombstone),
    {tombstone, {Head, Tombstone}}.

object_and_head(Clock) ->
    Object = riak_object:set_vclock(riak_object:new(?BUCKET, ?KEY, ?VALUE), Clock),
    Head = riak_object:convert_object_to_headonly(?BUCKET, ?KEY, Object),
    {val, {Head, Object}}.

gen_clocks([], _PL, _Order, Clocks) ->
    Clocks;
gen_clocks([{{_Idx, Node}, _Type} | Rest], PL, same,  [{_Owner, As} | _]=Acc) ->
    ?LET(NextRelationship, elements([same, later, conflict]),
             gen_clocks(Rest, PL, NextRelationship, [{Node, As} | Acc])
        );
gen_clocks([{{_Idx, Node}, _Type} | Rest], PL, later,  [{_Owner, Than} | _]=Acc) ->
    ?LET(NextRelationship, elements([same, later, conflict]),
         begin
             {{_, Actor}, _}  = hd(PL),
             Clock2 = vclock:increment(Actor, Than),
             gen_clocks(Rest, PL, NextRelationship, [{Node, Clock2} | Acc])
         end
        );
gen_clocks([{{_Idx, Node}, _Type} | Rest], PL, conflict,  [{Owner, With} | Clocks]) ->
    ?LET(NextRelationship, elements([same, later, conflict]),
         begin
             With2 = vclock:increment(Owner, With),
             Conflict = vclock:increment(Node, With),
             gen_clocks(Rest, PL, NextRelationship, [ {Node, Conflict}, {Owner, With2} | Clocks])
         end
        ).

%% given a PL (for actors) and a seed object (if there is one)
%% generate an object that is either before, same, after, or
%% concurrent with the seed. In the case of no seed then obviously
%% this object is after


%% keep N small, and likely, and realistic
gen_n() ->
    choose(2, 5).
%% generate valid Rs only
gen_r(N) ->
    choose(1, N).

gen_preflist(N) ->
    %% we just need N of them, content doesn't really matter
    [{{Index, make_node(Index)}, oneof([primary, fallback])} || Index <- lists:seq(1, N)].

%% -- helpers ---------------------------------------------------------------
setup_mocks(PL) ->
    meck:new(riak_kv_vnode),
    meck:expect(riak_kv_vnode, head, fun(Preflist, {?BUCKET, ?KEY}, ?REQID) when Preflist == PL ->
                                             ok
                                     end).

teardown_mocks() ->
    meck:unload(riak_kv_vnode).

make_node(N) ->
    list_to_atom("dev" ++ integer_to_list(N) ++ "@host").

fsm_state_data([]) ->
    undefined;
fsm_state_data([Latest | _Rest]) ->
    Latest.

next_result([]) ->
    undefined;
next_result([Res | _Rest]) ->
    Res.

%% TODO figure out what you're doing here!
calc_next_state(Results) ->
    case length(Results) of
        0 ->
            done;
        _ ->
            waiting_vnode_r
    end.
