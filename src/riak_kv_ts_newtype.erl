%% -------------------------------------------------------------------
%%
%% riak_kv_ts_newtype
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_ts_newtype).
-behaviour(gen_server).

%% API.
-export([
         new_type/1,
         start_link/0,
         recompile_ddl/1,
         retrieve_ddl_from_metadata/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
}).

-define(BUCKET_TYPE_PREFIX, {core, bucket_types}).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%%%
%%% API.
%%%

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec new_type(binary()) -> ok.
new_type(BucketType) ->
    lager:info("Add new Time Series bucket type ~s", [BucketType]),
    gen_server:cast(?MODULE, {new_type, BucketType}).

%%%
%%% gen_server.
%%%

init([]) ->
    process_flag(trap_exit, true),

    % do this outside of init so that we get crash messages output to crash.log
    % if it fails
    self() ! add_ddl_ebin_to_path,
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({new_type, BucketType}, State) ->
    ok = do_new_type(BucketType),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, normal}, State) ->
    % success
    lager:info("DDL Compilation Pid ~p successfully compiled", [Pid]),
    _ = riak_kv_compile_tab:update_state(Pid, compiled),
    {noreply, State};
handle_info({'EXIT', _, bucket_type_changed_mid_compile}, State) ->
    % this means that the process was interrupted while compiling by an update
    % to the metadata
    {noreply, State};
handle_info({'EXIT', Pid, _Error}, State) ->
    % compilation error, check
    lager:info("DDL Compilation Pid ~p failed", [Pid]),
    _ = riak_kv_compile_tab:update_state(Pid, failed),
    {noreply, State};
handle_info(add_ddl_ebin_to_path, State) ->
    ok = riak_core_metadata_manager:swap_notification_handler(
        ?BUCKET_TYPE_PREFIX, riak_kv_metadata_store_listener, []),
    ok = add_ddl_ebin_to_path(),
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%
%%% Internal.
%%%

%% We rely on the claimant to not give us new DDLs after the bucket
%% type is activated, at least until we have a system in place for
%% managing DDL versioning
do_new_type(Table) ->
    case retrieve_ddl_from_metadata(Table) of
        undefined ->
            log_missing_ddl_metadata(Table);
        DDL ->
            CurrentVersion = riak_ql_ddl:current_version(),
            TabResult = riak_kv_compile_tab:get_ddl(
                Table, CurrentVersion),
            case TabResult of
                notfound ->
                    try
                        %% this is a new DDL
                        DDLVersion = riak_ql_ddl:ddl_record_version(DDL),
                        case riak_ql_ddl:is_version_greater(CurrentVersion, DDLVersion) of
                            false ->
                                %% the version is too high, this is not happening!
                                ok;
                            true ->
                                ok
                        end,
                        log_compiling_new_type(Table),
                        DDLs = riak_ql_ddl:convert(CurrentVersion, DDL),
                        [riak_kv_compile_tab:insert(Table, DDLx) || DDLx <- DDLs],
                        %% TODO also insert downgraded versions as far back as we can go
                        actually_compile_ddl(Table, NewDDL)
                    catch
                        throw:unknown_version ->
                            log_unknown_ddl_version(Table, VersionAtom)
                    end;
                {ok, DDL} ->
                    log_new_type_is_duplicate(Table);
                {ok, StoredDDL} ->
                    %% there is a different DDL for the same table/version
                    %% FIXME recompile anyway? That is the current behaviour
                    ok
            end
    end.

%%
log_missing_ddl_metadata(Table) ->
    lager:info("No 'ddl' property in the metata for bucket type ~s",
        [Table]).

%%
log_compiling_new_type(Table) ->
    lager:info("Compiling new DDL for bucket type ~s with version ~p",
               [Table, riak_ql_ddl:current_version()]).

log_new_type_is_duplicate(Table) ->
    lager:info("Not compiling DDL for bucket type ~s because it is unchanged",
        [Table]).

%%
log_unknown_ddl_version(Table, VersionAtom) ->
        lager:error(
            "Unknown DDL version type ~p for bucket type ~s "
            "(expecting ~p with old version ~p, new version ~p)",
                [VersionAtom, Table, ?DDL_RECORD_NAME, OldVsn, NewVsn]).

% maybe_compile_ddl(BucketType, NewDDL, NewDDL, NewVsn, NewVsn) ->
%     lager:info("Not compiling DDL for bucket type ~s because it is unchanged", [BucketType]),
%     %% Do nothing; we're seeing a CMD update but the DDL hasn't changed
%     ok;
% maybe_compile_ddl(BucketType, NewDDL, NewDDL, NewVsn, OldVsn) when is_record(NewDDL, ?DDL_RECORD_NAME),
%                                                                    is_integer(NewVsn), is_integer(OldVsn) ->
%     lager:info("Recompiling same DDL for bucket type ~s from version ~b to ~b",
%                [BucketType, OldVsn, NewVsn]),
    
% maybe_compile_ddl(BucketType, NewDDL, _OldDDL, NewVsn, OldVsn) when is_record(NewDDL, ?DDL_RECORD_NAME),
%                                                                     is_integer(OldVsn), is_integer(NewVsn) ->
%     lager:info("Compiling new DDL for bucket type ~s from version ~b to ~b",
%                [BucketType, OldVsn, NewVsn]),
%     actually_compile_ddl(BucketType, NewDDL);
% maybe_compile_ddl(BucketType, NewDDL, _OldDDL, NewVsn, notfound) when is_record(NewDDL, ?DDL_RECORD_NAME),
%                                                                       is_integer(NewVsn) ->
%     lager:info("Compiling new DDL for bucket type ~s with version ~b",
%                [BucketType, NewVsn]),
%     actually_compile_ddl(BucketType, NewDDL);
% maybe_compile_ddl(BucketType, NewDDL, _OldDDL, NewVsn, OldVsn) ->
%     lager:error("Unknown DDL version type ~p for bucket type ~s "
%                 "(expecting ~p with old version ~p, new version ~p)",
%                 [NewDDL, BucketType, ?DDL_RECORD_NAME, OldVsn, NewVsn]),
%     %% We don't know what to do with this new DDL, so stop
%     ok.

%%
actually_compile_ddl(BucketType, NewDDL) ->
    ok = maybe_stop_current_compilation(BucketType),
    _Pid = start_compilation(BucketType, NewDDL),
    ok.

%%
maybe_stop_current_compilation(BucketType) ->
    case riak_kv_compile_tab:is_compiling(BucketType) of
        {true, CompilerPid} ->
            ok = stop_current_compilation(CompilerPid);
        false ->
            ok
    end.

%%
stop_current_compilation(CompilerPid) ->
    case is_process_alive(CompilerPid) of
        true ->
            exit(CompilerPid, bucket_type_changed_mid_compile),
            ok = flush_exit_message(CompilerPid);
        false ->
            ok
    end.

%%
flush_exit_message(CompilerPid) ->
    receive
        {'EXIT', CompilerPid, _} -> ok
    after
        1000 -> ok
    end.

%%
-spec start_compilation(BucketType::binary(), DDL::?DDL{}) -> pid().
start_compilation(BucketType, DDL) ->
    Pid = proc_lib:spawn_link(
        fun() ->
            ok = compile_and_store(ddl_ebin_directory(), DDL)
        end),
    lager:info("Starting DDL compilation of ~s on Pid ~p", [BucketType, Pid]),
    ok = riak_kv_compile_tab:insert(BucketType, riak_ql_ddl_compiler:get_compiler_version(), DDL, Pid, compiling),
    Pid.

%%
compile_and_store(BeamDir, DDL) ->
    case riak_ql_ddl_compiler:compile(DDL) of
        {error, _} = E ->
            E;
        {_, AST} ->
            {ok, ModuleName, Bin} = compile:forms(AST),
            ok = store_module(BeamDir, ModuleName, Bin)
    end.

%%
store_module(Dir, Module, Bin) ->
    Filepath = beam_file_path(Dir, Module),
    ok = filelib:ensure_dir(Filepath),
    ok = file:write_file(Filepath, Bin).

%%
beam_file_path(BeamDir, Module) ->
    filename:join(BeamDir, [Module, ".beam"]).

%% Return the directory where compiled DDL module beams are stored
%% before bucket type activation.
ddl_ebin_directory() ->
   DataDir = app_helper:get_env(riak_core, platform_data_dir),
   filename:join(DataDir, ddl_ebin).

%% Would be nice to have a function in riak_core_bucket_type or
%% similar to get either the prefix or the actual metadata instead
%% of including a riak_core header file for this prefix
retrieve_ddl_from_metadata(BucketType) when is_binary(BucketType) ->
    retrieve_ddl_2(riak_core_metadata:get(?BUCKET_TYPE_PREFIX, BucketType,
                                          [{allow_put, false}])).

%%
retrieve_ddl_2(undefined) ->
    undefined;
retrieve_ddl_2(Props) ->
    proplists:get_value(ddl, Props, undefined).

%%
add_ddl_ebin_to_path() ->
    Ebin_Path = ddl_ebin_directory(),
    ok = filelib:ensure_dir(filename:join(Ebin_Path, any)),
    % the code module ensures that there are no duplicates
    true = code:add_path(Ebin_Path),
    ok.

%%
-spec recompile_ddl(DDLVersion :: riak_ql_component:component_version()) -> ok.
recompile_ddl(DDLVersion) ->
    %% Get list of tables to recompile
    Tables = riak_kv_compile_tab:get_ddl_records_needing_recompiling(DDLVersion),
    lists:foreach(fun(Table) ->
                      new_type(Table)
                  end,
                  Tables),
    ok.

%% For each table
%%     Find the most recent version
%%     If the version is the most current one then continue
%%     Else upgrade it to the current version
%%     If the version is higher than the current version then continue (being downgraded, might not be able to check higher cos atoms)
% upgrade_tables() ->
%     Tables = riak_kv_compile_tab:get_all_table_names(),
%     CurrentVersion = riak_ql_ddl:current_version(),
%     ok.
    

% upgrade_table(Table, CurrentVersion) ->
%     [HighestVersion|Tail] = riak_kv_compile_tab:get_compiled_ddl_versions(Table),
%     case riak_ql_ddl:is_version_greater(CurrentVersion, HighestVersion) of
%         equal ->
%             %% the table is up to date, no need to upgrade
%             ok;
%         true ->
%             %% upgrade, current version is greater than our persisted
%             %% known versions
%             {ok, DDL} = riak_kv_compile_tab:get_ddl(Table, HighestVersion),
%             DDLs = riak_ql_ddl:convert(CurrentVersion, DDL),
%             [riak_kv_compile_tab:insert(Table, DDLx) || DDLx <- DDLs],
%             ok;
%         false ->
%             %% downgrade, the current version is lower than the latest
%             %% persisted version
%             ok
%     end.






