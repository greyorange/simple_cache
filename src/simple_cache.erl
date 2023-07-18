%%% @doc Main module for simple_cache.
%%%
%%% Copyright 2013 Marcelo Gornstein &lt;marcelog@@gmail.com&gt;
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%% @end
%%% @copyright Marcelo Gornstein <marcelog@gmail.com>
%%% @author Marcelo Gornstein <marcelog@gmail.com>
%%%
-module(simple_cache).
-author('marcelog@gmail.com').

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
-export([init/1]).
-export([get/4, get/5]).
-export([flush/1, flush/2, flush/3, clear/2, create_value/4]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Initializes a cache.
-spec init(string()) -> ok.
init(CacheName) ->
    CacheName = ets:new(CacheName, [
        named_table, {read_concurrency, true}, public, {write_concurrency, true}
    ]),
    ok.

%% @doc Deletes all keys in the given cache.
-spec flush(string()) -> true.
flush(CacheName) ->
    true = ets:delete_all_objects(CacheName).

%% @doc Deletes the keys that match the given ets:matchspec() from the cache.
-spec flush(string(), term()) -> true.
flush(CacheName, Key) ->
    ets:delete(CacheName, Key).

% for safe flushing of custom caching layer on top of mnesia
-spec flush(string(), term(), atom()) -> true.
flush(CacheName, {get_data_by_key, MnesiaKey, _Type} = Key, TableName) ->
    FlushFun =
        fun() ->
            mnesia:lock({TableName, MnesiaKey}, write),
            ets:delete(CacheName, Key)
        end,
    {atomic, Result} = db_functions:apply_transaction(FlushFun),
    Result;

flush(CacheName, Key, _TableName) ->
    ets:delete(CacheName, Key).

%% @doc Deletes the keys that match the given pattern from the cache.
-spec clear(string(), term()) -> true.
clear(CacheName, Pattern) ->
    ets:match_delete(CacheName, Pattern).

%% @doc Tries to lookup Key in the cache, and execute the given FunResult
%% on a miss.
-spec get(string(), infinity | pos_integer(), term(), function()) -> term().
get(CacheName, LifeTime, Key, FunResult) ->
    get(CacheName, LifeTime, Key, FunResult, #{}).

-spec get(string(), infinity | pos_integer(), term(), function(), map()) -> term().
get(CacheName, LifeTime, Key, FunResult, Options) ->
    CollectMetric = maps:get(collect_metric, Options, false),
    TableName = maps:get(table_name, Options, undefined),
    case ets:lookup(CacheName, Key) of
        [] ->
            % Not found, create it.
            collect_cache_metrics(CollectMetric, CacheName, 0),
            create_value(TableName, CacheName, LifeTime, Key, FunResult);
        [{Key, R, _CreatedTime, infinity}] ->
            collect_cache_metrics(CollectMetric, CacheName, 1),
            % Found, wont expire, return the value.
            R;
        [{Key, R, CreatedTime, LifeTime}] ->
            TimeElapsed = now_usecs() - CreatedTime,
            if
                TimeElapsed > (LifeTime * 1000) ->
                    % expired? create a new value
                    collect_cache_metrics(CollectMetric, CacheName, 0),
                    create_value(TableName, CacheName, LifeTime, Key, FunResult);
                true ->
                    collect_cache_metrics(CollectMetric, CacheName, 1),
                    % Not expired, return it.
                    R
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create_value(undefined, CacheName, LifeTime, Key, FunResult) ->
    create_value(CacheName, LifeTime, Key, FunResult);
create_value(TableName, CacheName, LifeTime, {get_data_by_key, MnesiaKey, _} = Key, FunResult) ->
    CreateFun =
        fun() ->
            mnesia:lock({TableName, MnesiaKey}, write),
            create_value(CacheName, LifeTime, Key, FunResult)
        end,
    {atomic, Result} = db_functions:apply_transaction(CreateFun),
    Result;
create_value(_TableName, CacheName, LifeTime, Key, FunResult) ->
    create_value(CacheName, LifeTime, Key, FunResult).

-spec create_value(string(), pos_integer() | infinity, term(), function()) -> term().
create_value(CacheName, infinity, Key, FunResult) ->
    create_value_internal(CacheName, infinity, Key, FunResult);
create_value(CacheName, LifeTime, Key, FunResult) ->
    Result = create_value_internal(CacheName, LifeTime, Key, FunResult),
    erlang:send_after(LifeTime, simple_cache_expirer, {expire, CacheName, Key}),
    Result.

create_value_internal(CacheName, LifeTime, Key, FunResult) ->
    R = FunResult(),
    ets:insert(CacheName, {Key, R, now_usecs(), LifeTime}),
    R.

%% @doc Returns total amount of microseconds since 1/1/1.
-spec now_usecs() -> pos_integer().
now_usecs() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    MegaSecs * 1000000000000 + Secs * 1000000 + MicroSecs.

collect_cache_metrics(false, _CacheName, _Val) ->
    ok;
collect_cache_metrics(true, CacheName, Val) ->
    prometheus_summary:observe(simple_cache_hit_boolean, [CacheName], Val).
