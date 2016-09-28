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
         is_compiled/2,
         new_type/1,
         start_link/0,
         recompile_ddl/0,
         retrieve_ddl_from_metadata/1,
         verify_helper_modules/0]).

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

%%
is_compiled(BucketType, DDL) when is_binary(BucketType) ->
    CurrentVersion = riak_ql_ddl:current_version(),
    (beam_exists(BucketType)
        andalso riak_kv_compile_tab:get_ddl(BucketType, CurrentVersion) == {ok, DDL}).
    % IsCompiled = (riak_kv_compile_tab:get_state(BucketType) == compiled andalso
    %               riak_kv_compile_tab:get_ddl(BucketType) == DDL).

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
            %% this bucket type name does not exist in the metadata!
            log_missing_ddl_metadata(Table);
        DDL ->
            CurrentVersion = riak_ql_ddl:current_version(),
            case riak_kv_compile_tab:get_ddl(Table, CurrentVersion) of
                notfound ->
                    try
                        ok = prepare_ddl_versions(CurrentVersion, DDL),
                        %% TODO also insert downgraded versions as far back as we can go
                        actually_compile_ddl(Table)
                    catch
                        throw:{unknown_ddl_version, _} ->
                            log_unknown_ddl_version(DDL),
                            ok
                    end;
                {ok, DDL} ->
                    log_new_type_is_duplicate(Table);
                {ok, _StoredDDL} ->
                    %% there is a different DDL for the same table/version
                    %% FIXME recompile anyway? That is the current behaviour
                    ok
            end
    end.

prepare_ddl_versions(CurrentVersion, ?DDL{ table = Table } = DDL) when is_binary(Table), is_atom(CurrentVersion) ->
    %% this is a new DDL
    DDLVersion = riak_ql_ddl:ddl_record_version(element(1, DDL)),
    case riak_ql_ddl:is_version_greater(CurrentVersion, DDLVersion) of
        false ->
            %% the version is too high, this is not happening!  another node
            %% with a higher version has somehow not respected the capability
            %% and sent a version we cannot handle
            throw(unknown_ddl_version);
        true ->
            ok;
        equal ->
            ok
    end,
    log_compiling_new_type(Table),
    %% conversions, if the DDL is greater, then we need to convert 
    DDLs = riak_ql_ddl:convert(CurrentVersion, DDL),
    lager:info("DDLs ~p", [DDLs]),
    [ok = riak_kv_compile_tab:insert(Table, DDLx) || DDLx <- DDLs],
    ok.

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
log_unknown_ddl_version(DDL) ->
    lager:error(
        "Unknown DDL version type ~p for bucket type ~s (current version is ~p)",
        [DDL, riak_ql_ddl:current_version()]).

%%
-spec actually_compile_ddl(BucketType::binary()) -> pid().
actually_compile_ddl(BucketType) ->
    lager:info("Starting DDL compilation of ~s", [BucketType]),
    Self = self(),
    Ref = make_ref(),
    Pid = proc_lib:spawn_link(
        fun() ->
            TabResult = riak_kv_compile_tab:get_ddl(BucketType, riak_ql_ddl:current_version()),
            {ModuleName, AST} = compile_to_ast(TabResult, BucketType),
            {ok, ModuleName, Bin} = compile:forms(AST),
            ok = store_module(ddl_ebin_directory(), ModuleName, Bin),
            Self ! {compilation_complete, Ref}
        end),
    receive
        {compilation_complete, Ref} ->
            lager:info("Compilation of DDL ~ts complete and stored to disk", [BucketType]),
            ok;
        {'EXIT', Pid, normal} ->
            ok;
        {'EXIT', Pid, Error} ->
            lager:error("Error compiling DDL ~ts with error ~p", [BucketType, Error]),
            ok
    after
        30000 ->
            %% really allow a lot of time to complete this, because there is
            %% not much we can do if it fails
            lager:error("timeout on compiling table ~s", [BucketType]),
            {error, timeout}
    end.

%% TODO fix return types
compile_to_ast(TabResult, BucketType) ->
    case TabResult of
        {ok, DDL} ->
            riak_ql_ddl_compiler:compile(DDL);
        _ ->
            merl:quote(disabled_table_source(binary_to_list(BucketType)))
    end.

%%
disabled_table_source(BucketType) when is_list(BucketType) ->
    "-module(" ++ BucketType ++ ").\n"
    "-compile(export_all).\n"
    "is_disabled() -> true.". %% TODO needs to be version number

%%
store_module(Dir, Module, Bin) ->
    Filepath = beam_file_path(Dir, Module),
    ok = filelib:ensure_dir(Filepath),
    lager:info("STORING BEAM ~p to ~p", [Module, Filepath]),
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

%% For each table
%%     Find the most recent version
%%     If the version is the most current one then continue
%%     Else upgrade it to the current version
%%     If the version is higher than the current version then continue (being downgraded, might not be able to check higher cos atoms)
recompile_ddl() ->
    Tables = riak_kv_compile_tab:get_all_table_names(),
    CurrentVersion = riak_ql_ddl:current_version(),
    [upgrade_ddl(T, CurrentVersion) || T <- Tables],
    ok.

%%
upgrade_ddl(Table, CurrentVersion) ->
    [HighestVersion|_] = riak_kv_compile_tab:get_compiled_ddl_versions(Table),
    case riak_ql_ddl:is_version_greater(CurrentVersion, HighestVersion) of
        equal ->
            %% the table is up to date, no need to upgrade
            ok;
        true ->
            %% upgrade, current version is greater than our persisted
            %% known versions

            {ok, DDL} = riak_kv_compile_tab:get_ddl(Table, HighestVersion),
            DDLs = riak_ql_ddl:convert(CurrentVersion, DDL),
            log_ddl_upgrade(DDL, DDLs),
            [riak_kv_compile_tab:insert(Table, DDLx) || DDLx <- DDLs],
            ok;
        false ->
            lager:info("WARNING NOT SUPPORTING DOWNGRADES YET", []),
            %% downgrade, the current version is lower than the latest
            %% persisted version, we have to hope there is a downgraded
            %% ddl in the compile tab
            ok
    end.

log_ddl_upgrade(DDL, DDLs) ->
    lager:info("UPGRADING ~p WITH ~p", [DDL, DDLs]).

%% For each table
%%     build the table name
%%     check if the beam is there for that module
%%     if not then build it
verify_helper_modules() ->
    [verify_helper_module(T) || T <- riak_kv_compile_tab:get_all_table_names()],
    ok.

verify_helper_module(Table) when is_binary(Table) ->
    case beam_exists(Table) of
        true ->
            lager:info("beam file for table ~ts exists", [Table]),
            ok;
        false ->
            lager:info("beam file for table ~ts must be recompiled", [Table]),
            actually_compile_ddl(Table)
    end.

%%
beam_exists(Table) when is_binary(Table) ->
    BeamDir = ddl_ebin_directory(),
    ModuleName = riak_ql_ddl:make_module_name(Table),
    filelib:is_file(beam_file_path(BeamDir, ModuleName)).





