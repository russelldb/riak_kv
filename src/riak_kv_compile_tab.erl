%% -------------------------------------------------------------------
%%
%% Store the state about what bucket type DDLs have been compiled.
%%
%% Copyright (c) 2015-2016 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_compile_tab).

-export([
         delete_dets/1,
         get_all_table_names/0,
         get_compiled_ddl_versions/1,
         get_ddl/2,
         get_state/2,
         get_ddl_records_needing_recompiling/1,
         insert/4,
         is_compiling/2,
         new/1,
         update_state/2]).

-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%% the table for TS 1.4 and below
-define(TABLE2, riak_kv_compile_tab_v2).
%% the table for TS 1.5
-define(TABLE3, riak_kv_compile_tab_v3).

-type compile_state() :: compiling | compiled | failed | retrying.
-export_type([compile_state/0]).

-define(is_compile_state(S),
        (S == compiling orelse
         S == compiled orelse
         S == failed orelse
         S == retrying)).

%% previous versions of the compile table used a plain tuple, and it was
%% difficult to different versions of the row tuple in the table for fear
%% of mixing them up. A versioned row record means several versions can
%% co-exist in the same table, and be matched in isolation.
%%
%% This row still requires a new table because the dets key index is
%% defaulted to 1.  All values default to underscores for simple matching.
-record(row_v3, {
    table = '_' :: binary(),
    %% dets key, a composite of the table name and ddl version
    table_version = '_' :: {binary(), riak_ql_ddl:ddl_version()},
    ddl = '_' :: riak_ql_ddl:any_ddl(),
    compiler_pid = '_' :: pid(),
    compile_state = '_' :: compile_state()
 }).

%%
-spec new(file:name()) -> {ok, dets:tab_name()} | error.
new(Dir) ->
    Options = [{type, set}, {repair, force}],
    {ok, _} = dets:open_file(?TABLE2, [{file, file_v2(Dir)} | Options]),
    {ok, _} = dets:open_file(?TABLE3, [{file, file_v3(Dir)}, {keypos, #row_v3.table_version } | Options]),
    mark_compiling_for_retry().

%% Useful testing tool
-spec delete_dets(file:name()) ->
    ok | {error, any()}.
delete_dets(FileDir) ->
    _ = dets:close(file_v3(FileDir)),
    _ = file:delete(file_v3(FileDir)).

%%
file_v2(Dir) ->
    filename:join(Dir, [?TABLE2, ".dets"]).

%%
file_v3(Dir) ->
    filename:join(Dir, [?TABLE3, ".dets"]).

%%
-spec insert(BucketType :: binary(),
             DDL :: term(),
             CompilerPid :: pid(),
             State :: compile_state()) -> ok | error.
insert(BucketType, DDL, CompilerPid, State) when is_binary(BucketType),
                                                 is_tuple(DDL) ->
    lager:info("DDL DETS Update: ~p, ~p, ~p, ~p",
               [BucketType, DDL, CompilerPid, State]),
    DDLVersion = riak_ql_ddl:ddl_record_version(element(1, DDL)),
    Row = #row_v3{
         table = BucketType
        ,table_version = {BucketType, DDLVersion}
        ,ddl = DDL
        ,compiler_pid = CompilerPid
        ,compile_state = State },
    ok = dets:insert(?TABLE3, Row),
    ok = dets:sync(?TABLE3),
    ok = insert_v2(BucketType, DDL, CompilerPid, State).

%% insert into the v2 table so that the record is available
insert_v2(BucketType, #ddl_v1{} = DDL, CompilerPid, State) ->
    %% the version is always 1 for the v2 table
    DDLVersion = 1,
    V2Row = {BucketType, DDLVersion, DDL, CompilerPid, State},
    dets:insert(?TABLE2, V2Row),
    ok = dets:sync(?TABLE2);
insert_v2(BucketType, DDL, CompilerPid, State) ->
    case riak_ql_ddl:convert(v1, DDL) of
        {error,_} ->
            %% this DDL cannot be downgraded and cannot be used in a 
            %% cluster containing a 1.4 node
            ok;
        DDLV1 ->
            insert_v2(BucketType, DDLV1, CompilerPid, State)
    end.

%% Check if the bucket type is in the compiling state.
-spec is_compiling(BucketType::binary(), Version::riak_ql:ddl_version()) ->
    {true, pid()} | false.
is_compiling(BucketType, Version) when is_binary(BucketType), is_atom(Version) ->
    case dets:lookup(?TABLE3, {BucketType,Version}) of
        [#row_v3{ compile_state = compiling, compiler_pid = Pid }] ->
            {true, Pid};
        _ ->
            false
    end.

%%
-spec get_state(BucketType :: binary(), Version::riak_ql:ddl_version()) ->
        compile_state() | notfound.
get_state(BucketType, Version) when is_binary(BucketType), is_atom(Version) ->
    case dets:lookup(?TABLE3, {BucketType,Version}) of
        [#row_v3{ compile_state = State }] ->
            State;
        [] ->
            notfound
    end.

%%
-spec get_compiled_ddl_versions(BucketType :: binary()) ->
    riak_ql_component:component_version() | notfound.
get_compiled_ddl_versions(BucketType) when is_binary(BucketType) ->
    MatchRow = #row_v3{
        table = BucketType,
        table_version = '$1' },
    case dets:match(?TABLE3, MatchRow) of
        [] ->
            notfound;
        TableVersions ->
            Versions = lists:map(fun([{_,V}]) -> V end, TableVersions),
            lists:sort(
                fun(A,B) ->
                    riak_ql_ddl:is_version_greater(A,B) == true
                end, Versions)
    end.

%%
-spec get_ddl(BucketType::binary(), Version::riak_ql_ddl:ddl_version()) ->
        term() | notfound.
get_ddl(BucketType, Version) when is_binary(BucketType), is_atom(Version) ->
    case dets:lookup(?TABLE3, {BucketType,Version}) of
        [#row_v3{ ddl = DDL }] ->
            DDL;
        [] ->
            notfound
    end.

%%
-spec get_all_table_names() -> [binary()].
get_all_table_names() ->
    Matches = dets:match(?TABLE3, #row_v3{ table = '$1',
                                           compile_state = compiled }),
    Tables = [Table || [Table] <- Matches],
    lists:usort(Tables).
-include_lib("eunit/include/eunit.hrl").

%% Update the compilation state using the compiler pid as a key.
%% Since it has an active Pid, it is assumed to have a DDL version already.
-spec update_state(CompilerPid :: pid(), State :: compile_state()) ->
        ok | error | notfound.
update_state(CompilerPid, State) when is_pid(CompilerPid),
                                      ?is_compile_state(State) ->
    MatchRow = #row_v3{
        table = '$1',
        ddl = '$2',
        compiler_pid = CompilerPid },

    ?debugFmt("UPDATE STATE ~p", [MatchRow]),
    dets:traverse(?TABLE3, fun(X) -> ?debugFmt("~p", [X]) end),

    case dets:match(?TABLE3, MatchRow) of
        [[BucketType, DDL]] ->
            insert(BucketType, DDL, CompilerPid, State);
        [] ->
            notfound
    end.

% %% Mark any lingering compilations as being retried
-spec mark_compiling_for_retry() -> ok.
mark_compiling_for_retry() ->
    CompilingPids = dets:match(?TABLE3, {'_','_','_','$1',compiling}),
    lists:foreach(
        fun([Pid]) ->
            update_state(Pid, retrying)
        end, CompilingPids).

%% Get the list of records which need to be recompiled
-spec get_ddl_records_needing_recompiling(DDLVersion :: riak_ql_component:component_version()) ->
    [binary()].
get_ddl_records_needing_recompiling(DDLVersion) ->
    %% First find all tables with a version
    MismatchedTables = dets:select(?TABLE3, [{{'$1','$2','_','_',compiled},[{'/=','$2', DDLVersion}],['$$']}]),
    RetryingTables = dets:match(?TABLE3, {'$1','$2','_','_',retrying}),
    Tables = [hd(X) || X <- MismatchedTables ++ RetryingTables],
    lager:info("Recompile the DDL of these bucket types ~p", [Tables]),
    Tables.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(in_process(TestCode),
    Self = self(),
    spawn_monitor(
        fun() ->
            _ = riak_kv_compile_tab:delete_dets("."),
            _ = riak_kv_compile_tab:new("."),
            TestCode,
            Self ! test_ok
        end),
    receive
        test_ok -> ok;
        {'DOWN',_,_,_,normal} -> ok;
        {'DOWN',_,_,_,Error} -> error(Error);
        Error -> error(Error)
    end
).

insert_test() ->
    ?in_process(
        begin
            Pid = spawn(fun() -> ok end),
            ok = insert(<<"my_type">>, #ddl_v2{local_key = #key_v1{ }}, Pid, compiling),
            ?assertEqual(
                compiling,
                get_state(<<"my_type">>, v2)
            )
        end).

update_state_test() ->
    ?in_process(
        begin
            Pid = spawn(fun() -> ok end),
            ok = insert(<<"my_type">>, #ddl_v1{local_key = #key_v1{ }}, Pid, compiling),
            ok = update_state(Pid, compiled),
            ?assertEqual(
                compiled,
                get_state(<<"my_type">>, v1)
            )
        end).

is_compiling_test() ->
    ?in_process(
        begin
            Type = <<"is_compiling_type">>,
            Pid = spawn(fun() -> ok end),
            ok = insert(Type, #ddl_v1{local_key = #key_v1{ }}, Pid, compiling),
            ?assertEqual(
                {true, Pid},
                is_compiling(Type, v1)
            )
        end).

compiled_version_test() ->
    ?in_process(
        begin
            Pid = spawn(fun() -> ok end),
            ok = insert(<<"my_type">>, #ddl_v1{local_key = #key_v1{ }}, Pid, compiled),
            ?assertEqual(
                [v1],
                get_compiled_ddl_versions(<<"my_type">>)
            )
        end).

get_ddl_test() ->
    ?in_process(
        begin
            Pid = spawn(fun() -> ok end),
            ok = insert(<<"my_type">>, #ddl_v1{local_key = #key_v1{ }}, Pid, compiled),
            ?assertEqual(
                #ddl_v1{local_key = #key_v1{ }},
                get_ddl(<<"my_type">>, v1)
            )
        end).

recompile_ddl_test() ->
    ?in_process(
        begin
            DDLV1 = #ddl_v1{local_key = #key_v1{ }},
            Pid = spawn(fun() -> ok end),
            Pid2 = spawn(fun() -> ok end),
            Pid3 = spawn(fun() -> ok end),
            Pid4 = spawn(fun() -> ok end),
            ok = insert(<<"my_type1">>, DDLV1, Pid, compiling),
            ok = insert(<<"my_type2">>, DDLV1, Pid2, compiled),
            ok = insert(<<"my_type3">>, DDLV1, Pid3, compiling),
            ok = insert(<<"my_type4">>, DDLV1, Pid4, compiled),
            mark_compiling_for_retry(),
            ?assertEqual(
                [<<"my_type1">>,
                 <<"my_type2">>,
                 <<"my_type3">>
                ],
                lists:sort(get_ddl_records_needing_recompiling(8))
            )
        end).

get_all_compiled_ddls_test() ->
    ?in_process(
        begin
            Pid = spawn(fun() -> ok end),
            Pid2 = spawn(fun() -> ok end),
            Pid3 = spawn(fun() -> ok end),
            Pid4 = spawn(fun() -> ok end),
            ok = insert(<<"my_type1">>, {ddl_v1}, Pid, compiling),
            ok = insert(<<"my_type2">>, {ddl_v1}, Pid2, compiled),
            ok = insert(<<"my_type3">>, {ddl_v1}, Pid3, compiling),
            ok = insert(<<"my_type4">>, {ddl_v1}, Pid4, compiled),

            ?assertEqual(
                [<<"my_type2">>,<<"my_type4">>],
                get_all_table_names()
            )
        end).
-endif.
