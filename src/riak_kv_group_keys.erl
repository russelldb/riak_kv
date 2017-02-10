%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_group_keys).
-export([fold_keys/6]).

-record(group_params, {
          backend_mod :: module(),
          prefix :: binary(),
          delimiter :: binary(),
          start_after :: binary(),
          max_keys :: pos_integer()
         }).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

fold_keys(BackendMod, FoldFun, Acc, Opts, FoldOpts, DbRef) ->
    Bucket = proplists:get_value(bucket, Opts),
    GroupParams = to_group_params(BackendMod, proplists:get_value(group_params, Opts, [])),
    ContentFolderFun = content_folder_fun(Bucket, GroupParams, DbRef, FoldOpts, FoldFun, Acc),
    Async = proplists:get_bool(async_fold, Opts),
    case Async of
        true ->
            {async, ContentFolderFun};
        false ->
            {ok, ContentFolderFun()}
    end.

to_group_params(BackendMod, PropList) ->
    #group_params{
       backend_mod=BackendMod,
       prefix=value_or_default(prefix, PropList, undefined),
       delimiter=value_or_default(delimiter, PropList, undefined),
       start_after=value_or_default(start_after, PropList, undefined),
       max_keys=value_or_default(max_keys, PropList, 1000)
      }.

value_or_default(Key, PropList, Default)->
    case proplists:get_value(Key, PropList, "") of
        <<"">> ->
            Default;
        "" ->
            Default;
        Value ->
            Value
    end.

backend_from(#group_params{backend_mod = BackendMod}) ->
    BackendMod.

content_folder_fun(Bucket, GroupParams, DbRef, FoldOpts, FoldFun, Acc) ->
    BackendMod = backend_from(GroupParams),
    fun() ->
            FoldOpts1 = [{first_key, BackendMod:to_first_key({bucket, Bucket})} | FoldOpts],
            try iterator_open(DbRef, FoldOpts1) of
                {ok, Itr} ->
                    iterate(Bucket, GroupParams, Itr, FoldFun, Acc)
            catch Error ->
                    lager:debug("Could not open iterator: ~p", [Error]),
                    throw(Error)
            end
    end.

iterator_open(DbRef, FoldOpts) ->
    eleveldb:iterator(DbRef, FoldOpts).

iterator_close(Itr) ->
    eleveldb:iterator_close(Itr).

iterator_move(Itr, Pos) ->
    eleveldb:iterator_move(Itr, Pos).

iterate(Bucket, GroupParams, Itr, FoldFun, Acc) ->
    Outcome = enumerate(undefined, Bucket, GroupParams, Itr, FoldFun, Acc),
    iterator_close(Itr),
    case Outcome of
        {error, _Err} = Error ->
            throw(Error);
        {break, _Res} = Result ->
            throw(Result);
        Result -> Result
    end.

enumerate(PrevEntry, Bucket, GroupParams, Itr, FoldFun, Acc) ->
    Pos = next_pos(PrevEntry, Bucket, GroupParams),
    BackendMod = backend_from(GroupParams),
    case Pos of
        npos ->
            Acc;
        _ ->
            try iterator_move(Itr, Pos) of
                {error, invalid_iterator} ->
                    lager:debug( "invalid_iterator.  reached end.", []),
                    Acc;
                {error, iterator_closed} ->
                    lager:debug( "iterator_closed.", []),
                    Acc;
                {ok, BinaryBKey, BinaryValue} ->
                    BKey = BackendMod:from_object_key(BinaryBKey),
                    lager:info("BKey: ~p", [BKey]),
                    maybe_accumulate({BKey, BinaryValue}, PrevEntry, Bucket, GroupParams, FoldFun, Acc, Itr)
            catch Error ->
                    {error, {eleveldb_error, Error}}
            end
    end.

start_pos(Bucket, GroupParams = #group_params{prefix=undefined, start_after=undefined}) ->
    to_object_key(GroupParams, Bucket, <<"">>);
start_pos(Bucket, GroupParams = #group_params{prefix=Prefix, start_after=undefined}) ->
    to_object_key(GroupParams, Bucket, Prefix);
start_pos(Bucket, GroupParams = #group_params{prefix=undefined, start_after=StartAfter}) ->
    ObjectKey = to_object_key(GroupParams, Bucket, append_null_byte(StartAfter)),
    lager:info("StartAfter: ~p~nObjectKey: ~p", [StartAfter, riak_kv_eleveldb_backend:from_object_key(ObjectKey)]),
    ObjectKey;
start_pos(Bucket, GroupParams = #group_params{prefix=Prefix, start_after=StartAfter}) ->
    case StartAfter =< Prefix of
        true ->
            to_object_key(GroupParams, Bucket, Prefix);
        _ ->
            npos
    end.

next_pos(undefined, Bucket, GroupParams) ->
    start_pos(Bucket, GroupParams);
next_pos({Bucket, PrevKey}, _Bucket, GroupParams =  #group_params{prefix=undefined}) ->
    next_pos_after_key(GroupParams, Bucket, PrevKey);
next_pos({Bucket, PrevKey}, _Bucket, GroupParams =  #group_params{prefix=Prefix}) ->
    case is_prefix(Prefix, PrevKey) of
        true ->
            next_pos_after_key(GroupParams, Bucket, PrevKey);
        _ ->
            npos
    end.

next_pos_after_key(#group_params{prefix = undefined, delimiter = undefined}, _Bucket, _Key) ->
    next;
next_pos_after_key(GroupParams = #group_params{prefix = Prefix, delimiter = Delimiter}, Bucket, Key) ->
    case common_prefix(Key, Prefix, Delimiter) of
        undefined ->
            next;
        CommonPrefix ->
            to_object_key(GroupParams, Bucket, append_max_byte(CommonPrefix))
    end.

is_prefix(B1, B2) when is_binary(B1), is_binary(B2) ->
    binary:longest_common_prefix([B1, B2]) == size(B1);
is_prefix(_B1, _B2) ->
    false.

to_object_key(GroupParams, Bucket, Key) ->
    BackendMod = backend_from(GroupParams),
    BackendMod:to_object_key(Bucket, Key).

maybe_accumulate({undefined, _BinaryValue}, _PrevEntry, _Bucket, _GroupParams, _FoldFun, Acc, _Itr) ->
    Acc;
maybe_accumulate({ignore, _BinaryValue}, _PrevEntry, _Bucket, _GroupParams, _FoldFun, Acc, _Itr) ->
    lager:error("Encountered corrupt key while iterating entries. Grouped key list may be incomplete."),
    Acc;
maybe_accumulate(PrevEntry, PrevEntry, _Bucket, _GroupParams, _FoldKeysFun, _Acc, _Itr) ->
    {error, did_not_skip_to_next_entry};
maybe_accumulate({{Bucket, _Key}, _BinaryValue} = Entry, _PrevEntry, Bucket, #group_params{prefix = undefined, max_keys = MaxKeys}=GroupParams, FoldFun, Acc, Itr) ->
    case riak_kv_fold_buffer:size(Acc) =< MaxKeys of
        true ->
            lager:info("Arrived at MaxKeys of ~p: ", [MaxKeys]),
            accumulate(Entry, Bucket, GroupParams, FoldFun, Acc, Itr);
        false -> Acc
    end;
maybe_accumulate(_Entry, _PrevEntry, _Bucket, #group_params{prefix=undefined}=_GroupParams, _FoldFun, Acc, _Itr) ->
    Acc;
maybe_accumulate({{Bucket, Key}, _BinaryValue} = Entry, _PrevEntry, Bucket, #group_params{prefix = Prefix, max_keys = MaxKeys}=GroupParams, FoldFun, Acc, Itr) ->
    case Prefix =/= undefined andalso is_prefix(Prefix, Key) of
        true ->
            case riak_kv_fold_buffer:size(Acc) =< MaxKeys of
                true ->
                    lager:info("Arrived at MaxKeys of ~p: ", [MaxKeys]),
                    accumulate(Entry, Bucket, GroupParams, FoldFun, Acc, Itr);
                false -> Acc
            end;
        false ->
                                                % done
            Acc
    end;
maybe_accumulate({{_DifferentBucket, _Key}, _BinaryValue}, _PrevEntry, _Bucket, _GroupParams, _FoldFun, Acc, _Itr) ->
    Acc.

%% Note that the two instances of `TargetBucket' below are intentional so that we accumulate only when
%% we match the bucket that we're looking for.
accumulate({{TargetBucket, Key}=BKey, BinaryValue},
           TargetBucket,
           GroupParams,
           FoldFun,
           Acc,
           Itr) ->
    NewAcc = try
                 PrefixOrMeta = common_prefix_or_metadata(BKey, BinaryValue, GroupParams),
                 FoldFun(TargetBucket, Key, PrefixOrMeta, Acc)
             catch Error ->
                     FoldFun(TargetBucket, Key, {error, Error}, Acc)
             end,
    enumerate(BKey, TargetBucket, GroupParams, Itr, FoldFun, NewAcc).

common_prefix_or_metadata({_Bucket, Key} = BKey,
                          BinaryValue,
                          #group_params{prefix = Prefix, delimiter = Delimiter}) ->

    case common_prefix(Key, Prefix, Delimiter) of
        undefined -> {metadata, extract_metadata(BKey, BinaryValue)};
        CommonPrefix -> {common_prefix, CommonPrefix}
    end.

common_prefix(_Key, _Prefix, undefined) ->
    undefined;
common_prefix(_Key, _Prefix, <<>>) ->
    undefined;
common_prefix(Key, undefined, Delimiter) ->
    common_prefix(Key, <<>>, Delimiter);
common_prefix(Key, Prefix, Delimiter) ->
    Index = binary:longest_common_prefix([Key, Prefix]),
    KeySansPrefix = binary_part(Key, Index, byte_size(Key) - Index),
    Parts = binary:split(KeySansPrefix, Delimiter, [global]),
    case length(Parts) > 1 andalso lists:all(fun(X) -> X =/= <<>> end, Parts) of
        true ->
            CommonPrefix = hd(Parts),
            <<Prefix/binary, CommonPrefix/binary, Delimiter/binary>>;
        false ->
            undefined
    end.

extract_metadata({Bucket, Key} = _BKey,
                 BinaryValue) ->
    RObj = riak_object:from_binary(Bucket, Key, BinaryValue),
    Contents = riak_object:get_contents(RObj),
    case Contents of
        [_Content] ->
            riak_object:get_metadata(RObj);
        _ ->
            %% TODO
            {error, riak_object_has_siblings}
    end.

-spec append_null_byte(binary()) -> binary().
append_null_byte(Binary) when is_binary(Binary) ->
    <<Binary/binary, 0>>.

%% 0xFF is the biggest byte for the purposes of sorting in LevelDB,
%% used for seeking past all keys starting with the prefix.
-spec append_max_byte(binary()) -> binary().
append_max_byte(Binary) when is_binary(Binary) ->
    <<Binary/binary, 16#FF>>.

%% ====================
%% TESTS
%% ====================

-ifdef(TEST).

test_group_params(Prefix, Delimiter) ->
    to_group_params(riak_kv_eleveldb_backend, [{prefix, Prefix}, {delimiter, Delimiter}]).

expected_next_pos(Bucket, Key) ->
    riak_kv_eleveldb_backend:to_object_key(Bucket, append_max_byte(Key)).

common_prefix_test() ->
    Key = <<"foo/bar/baz">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = <<"foo/bar/">>,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_no_delimiter_test() ->
    Key = <<"foo/bar">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = undefined,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_key_equal_prefix_test() ->
    Key = <<"foo/">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = undefined,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_invalid_terminating_delimiter_test() ->
    Key = <<"foo/bar/">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = undefined,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_empty_segment_test() ->
    Key = <<"foo//bar/baz">>,
    Prefix = <<"foo/">>,
    Delimiter = <<"/">>,
    Expected = undefined,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_empty_prefix_test() ->
    Key = <<"foo/bar/baz">>,
    Prefix = <<"">>,
    Delimiter = <<"/">>,
    Expected = <<"foo/">>,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_multi_char_delimiter_test() ->
    Key = <<"foo::bar::baz">>,
    Prefix = <<"foo::">>,
    Delimiter = <<"::">>,
    Expected = <<"foo::bar::">>,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_with_emoji_delimiter_test() ->
    Key = <<"foo🤔bar🤔baz">>,
    Prefix = <<"foo🤔">>,
    Delimiter = <<"🤔">>,
    Expected = <<"foo🤔bar🤔">>,
    Result = common_prefix(Key, Prefix, Delimiter),
    ?assertEqual(Expected, Result).

common_prefix_or_metadata_returns_metadata_test() ->
    Bucket = {<<"bucket_type">>, <<"bucket">>},
    Key  = <<"foo/actual_object">>,
    BKey = {Bucket, Key},
    RObj = riak_object:new(Bucket, Key,<<"value">>),
    GroupParams = #group_params{prefix = <<"foo/">>, delimiter = <<"/">>},
    ?assertMatch({metadata, _Anything}, common_prefix_or_metadata(BKey, RObj, GroupParams)).

common_prefix_or_metadata_returns_common_prefix_test() ->
    Bucket = {<<"bucket_type">>, <<"bucket">>},
    Key  = <<"foo/prefix/actual_object">>,
    BKey = {Bucket, Key},
    RObj = riak_object:new(Bucket, Key,<<"value">>),
    GroupParams = #group_params{prefix = <<"foo/">>, delimiter = <<"/">>},
    ?assertMatch({common_prefix, <<"foo/prefix/">>}, common_prefix_or_metadata(BKey, RObj, GroupParams)).

common_prefix_or_metadata_with_undefined_delimiter_test() ->
    Bucket = {<<"bucket_type">>, <<"bucket">>},
    Key  = <<"foo/prefix/actual_object">>,
    BKey = {Bucket, Key},
    RObj = riak_object:new(Bucket, Key,<<"value">>),
    GroupParams = #group_params{prefix = <<"foo/">>},
    ?assertMatch({metadata, _Anything}, common_prefix_or_metadata(BKey, RObj, GroupParams)).

next_pos_after_key_with_no_prefix_or_delimiter_test() ->
    GroupParams = test_group_params(undefined, undefined),
    Bucket = <<"bucket">>,
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo">>)),
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo/bar">>)),
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo/bar/baz">>)).

next_pos_after_key_with_delimiter_test() ->
    GroupParams = test_group_params(undefined, <<"/">>),
    Bucket = <<"bucket">>,
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo">>)),
    ?assertEqual(expected_next_pos(Bucket, <<"foo/">>),
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar">>)),
    ?assertEqual(expected_next_pos(Bucket, <<"foo/">>),
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar/baz">>)).

next_pos_after_key_with_prefix_test() ->
    GroupParams = test_group_params(<<"foo/">>, undefined),
    Bucket = <<"bucket">>,
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo">>)),
    ?assertEqual(next,
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar">>)),
    ?assertEqual(next,
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar/baz">>)).

next_pos_after_key_with_prefix_and_delimiter_test() ->
    GroupParams = test_group_params(<<"foo/">>, <<"/">>),
    Bucket = <<"bucket">>,
    ?assertEqual(next, next_pos_after_key(GroupParams, Bucket, <<"foo">>)),
    ?assertEqual(next,
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar">>)),
    ?assertEqual(expected_next_pos(Bucket, <<"foo/bar/">>),
                 next_pos_after_key(GroupParams, Bucket, <<"foo/bar/baz">>)).

-endif.
