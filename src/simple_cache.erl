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
-export([flush/1, flush/2, clear/2, create_value/4]).

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

%% @doc Deletes the keys that match the given ets:matchspec() from the cache.
-spec flush(string(), term()) -> true.
flush(CacheName, Key) ->
  ets:delete(CacheName, Key).

%% @doc Deletes the keys that match the given pattern from the cache.
-spec clear(string(), term()) -> true.
clear(CacheName, Pattern) ->
  ets:match_delete(CacheName, Pattern).

%% @doc Deletes all keys in the given cache.
-spec flush(string()) -> true.
flush(CacheName) ->
  true = ets:delete_all_objects(CacheName).

%% @doc Tries to lookup Key in the cache, and execute the given FunResult
%% on a miss.
-spec get(string(), infinity|pos_integer(), term(), function()) -> term().
get(CacheName, LifeTime, Key, FunResult) ->
  get(CacheName, LifeTime, Key, FunResult, #{}).

-spec get(string(), infinity|pos_integer(), term(), function(), map()) -> term().
get(CacheName, LifeTime, Key, FunResult, Options) ->
  CollectMetric = maps:get(collect_metric, Options, false),
  case ets:lookup(CacheName, Key) of
    [] ->
      % Not found, create it.
      case CollectMetric of
        true -> prometheus_summary:observe(simple_cache_hit_boolean, [CacheName], 0);
        false -> ok
      end,
      create_value(CacheName, LifeTime, Key, FunResult);
    [{Key, R, _CreatedTime, infinity}] ->
      case CollectMetric of
        true -> prometheus_summary:observe(simple_cache_hit_boolean, [CacheName], 1);
        false -> ok
      end,
      R; % Found, wont expire, return the value.
    [{Key, R, CreatedTime, LifeTime}] ->
      TimeElapsed = now_usecs() - CreatedTime,
      if
        TimeElapsed > (LifeTime * 1000) ->
          % expired? create a new value
          case CollectMetric of
            true -> prometheus_summary:observe(simple_cache_hit_boolean, [CacheName], 0);
            false -> ok
          end,
          case maps:get(flush_after_ttl, Options, false) of
            false ->
              create_value(CacheName, LifeTime, Key, FunResult);
            teue ->
              flush(CacheName, Key),
              {error, expired}
          end;
        true ->
          case CollectMetric of
            true -> prometheus_summary:observe(simple_cache_hit_boolean, [CacheName], 1);
            false -> ok
          end,
          R % Not expired, return it.
      end
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Creates a cache entry.
-spec create_value(string(), pos_integer(), term(), function()) -> term().
create_value(CacheName, LifeTime, Key, FunResult) ->
  R = FunResult(),
  ets:insert(CacheName, {Key, R, now_usecs(), LifeTime}),
  R.

%% @doc Returns total amount of microseconds since 1/1/1.
-spec now_usecs() -> pos_integer().
now_usecs() ->
  {MegaSecs, Secs, MicroSecs} = os:timestamp(),
  MegaSecs * 1000000000000 + Secs * 1000000 + MicroSecs.
