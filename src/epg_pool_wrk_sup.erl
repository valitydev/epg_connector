-module(epg_pool_wrk_sup).

-behaviour(supervisor).

%% API
-export([start_link/3]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(PoolName, DbParams, Size) ->
    RegName = reg_name(PoolName, "_pool_wrk_sup"),
    supervisor:start_link({local, RegName}, ?MODULE, [PoolName, DbParams, Size]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([PoolName, DbParams, Size]) ->
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => MaxRestarts,
        period => MaxSecondsBetweenRestarts
    },
    ChildSpecs = [
        #{
            id => reg_name(PoolName, "_pool_wrk"),
            start => {epg_pool_wrk, start_link, [PoolName, DbParams, Size]}
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.

%%

reg_name(Name, Postfix) ->
    list_to_atom(atom_to_list(Name) ++ Postfix).
