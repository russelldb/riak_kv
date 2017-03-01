-module(riak_kv_put_fsm_comm).

%% API
-export([start_state/1,
         schedule_request_timeout/1,
         start_remote_coordinator/3]).


start_state(StateName) ->
    gen_fsm:send_event(self(), {start, StateName}).

schedule_request_timeout(infinity) ->
    undefined;
schedule_request_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

start_remote_coordinator(CoordNode, Args, CoordinatorTimeout) ->
    %% If the net_kernel cannot talk to CoordNode, then any variation
    %% of the spawn BIF will block.  The whole point of picking a new
    %% coordinator node is being able to pick a new coordinator node
    %% and try it ... without blocking for dozens of seconds.
    RemoteRef = make_ref(),
    FsmPid = self(),
    Pid = spawn(fun() ->
                    RemoteCoordinatorPid = proc_lib:spawn(CoordNode, riak_kv_put_fsm, start_link, Args),
                    riak_kv_put_fsm:remote_coordinator_started(FsmPid, RemoteRef, RemoteCoordinatorPid)
                end),
    TRef = erlang:start_timer(CoordinatorTimeout, self(), coordinator_timeout),
    {Pid, TRef, RemoteRef}.
