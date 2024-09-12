-module(epg_pool_sup).

-behaviour(supervisor).

-export([start_link/3]).

-export([init/1]).

start_link(PoolName, DbParams, Size) ->
    RegName = reg_name(PoolName, "_pool_sup"),
    supervisor:start_link({local, RegName}, ?MODULE, [PoolName, DbParams, Size]).

init([PoolName, DbParams, Size]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1000,
        period => 3600
    },
    ConnectionsSupSpec = #{
        id => reg_name(PoolName, "_pool_wrk_sup"),
        start => {epg_pool_wrk_sup, start_link, [PoolName, DbParams, Size]},
        type => supervisor
    },
    ManagerSpec = #{
        id => reg_name(PoolName, "_pool_mgr"),
        start => {epg_pool_mgr, start_link, [PoolName, DbParams, Size]}
    },
    Specs = [
        ConnectionsSupSpec,
        ManagerSpec
    ],
    {ok, {SupFlags, Specs}}.

%% internal functions

reg_name(Name, Postfix) ->
    list_to_atom(atom_to_list(Name) ++ Postfix).
