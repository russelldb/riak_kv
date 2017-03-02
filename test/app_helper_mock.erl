%%% This module is to be used as a fallback module when doing mocking with EQC.
%%% It allows us to leave the calls to app_helper out of the callout specifications
%%% as they add nothing to the problem we need to validate.

-module(app_helper_mock).

-export([get_env/2,
         get_env/3]).


get_env(riak_kv, fsm_trace_enabled) ->
    false;
get_env(riak_core, default_bucket_props) ->
    [{chash_keyfun, {riak_core_util, chash_std_keyfun}},
     {pw, 0},
     {w, quorum},
     {dw, quorum}].


get_env(riak_kv, put_coordinator_failure_timeout, 3000) ->
    3000;
get_env(riak_kv, retry_put_coordinator_failure, true) ->
    true.
