%%%-------------------------------------------------------------------
%% @doc epg_connector top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(epg_connector_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).
-export([pool_specs/2]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    Databases = application:get_env(epg_connector, databases, #{}),
    Pools = application:get_env(epg_connector, pools, #{}),
    PoolSpecs = pool_specs(Pools, Databases),
    {ok, {SupFlags, PoolSpecs}}.

-spec pool_specs(epg_connector_app:pools(), epg_connector_app:databases()) -> supervisor:child_spec().
pool_specs(Pools, Databases) ->
    maps:fold(
        fun(PoolName, Opts, Acc) ->
            #{
                database := DB,
                size := Size
            } = Opts,
            DbParams = maps:get(DB, Databases),
            [
                #{
                    id => PoolName,
                    start => {epg_pool_sup, start_link, [PoolName, DbParams, Size]},
                    type => supervisor
                }
                | Acc
            ]
        end,
        [],
        Pools
    ).
