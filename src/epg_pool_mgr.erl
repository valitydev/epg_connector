-module(epg_pool_mgr).

-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-export([checkout/1]).
-export([checkin/3]).
-export([add/3]).
-export([remove/3]).

-record(epg_pool_mgr_state, {pool, params, size, connections, workers, owners, monitors}).

%% API

checkout(Pool) ->
    RegName = reg_name(Pool, "_pool_mgr"),
    gen_server:call(RegName, checkout).

checkin(Pool, Owner, Connection) ->
    RegName = reg_name(Pool, "_pool_mgr"),
    gen_server:cast(RegName, {checkin, Owner, Connection}).

add(Pool, Worker, Connection) ->
    RegName = reg_name(Pool, "_pool_mgr"),
    gen_server:cast(RegName, {add, Worker, Connection}).

remove(Pool, Worker, Connection) ->
    RegName = reg_name(Pool, "_pool_mgr"),
    gen_server:cast(RegName, {remove, Worker, Connection}).

%%

start_link(PoolName, DbParams, Size) ->
    RegName = reg_name(PoolName, "_pool_mgr"),
    gen_server:start_link({local, RegName}, ?MODULE, [PoolName, DbParams, Size], []).

init([PoolName, DbParams, Size]) ->
    _ = start_workers(PoolName, Size),
    {ok, #epg_pool_mgr_state{
        pool = PoolName,
        params = DbParams,
        size = Size,
        connections = queue:new(),
        workers = #{},
        owners = #{},
        monitors = #{}
    }}.
%%

handle_call(
    checkout, {Pid, _Ref},
    State = #epg_pool_mgr_state{owners = Owners}
) when erlang:is_map_key(Pid, Owners)->
    {reply, {error, nested_checkout}, State};
handle_call(
    checkout, {Pid, _Ref},
    State = #epg_pool_mgr_state{connections = Conns, owners = Owners}
) ->
    {Result, NewConns} = queue:out(Conns),
    {State1, Response} =
        case Result of
            {value, Connection} ->
                Ref = erlang:monitor(process, Pid),
                {State#epg_pool_mgr_state{owners = Owners#{Pid => {Ref, Connection}}}, Connection};
            empty ->
                {State, empty}
        end,
    NewState = State1#epg_pool_mgr_state{connections = NewConns},
    {reply, Response, NewState}.

handle_cast(
    {add, Worker, Connection},
    State = #epg_pool_mgr_state{connections = Conns, workers = Workers, monitors = Monitors}
) ->
    _ = erlang:monitor(process, Worker),
    MRef = erlang:monitor(process, Connection),
    {
        noreply,
        State#epg_pool_mgr_state{
            connections = unique_queue_push(Connection, Conns),
            workers = Workers#{Worker => Connection},
            monitors = Monitors#{Connection => MRef}
        }
    };
handle_cast(
    {remove, Worker, Connection},
    State = #epg_pool_mgr_state{connections = Conns, workers = Workers, monitors = Monitors}
) ->
    NewConns = queue:delete(Connection, Conns),
    NewWorkers = maps:without([Worker], Workers),
    NewMonitors = demonitor_and_close(Connection, Monitors),
    {
        noreply,
        State#epg_pool_mgr_state{connections = NewConns, workers = NewWorkers, monitors = NewMonitors}
    };
handle_cast(
    {checkin, Owner, Connection},
    State = #epg_pool_mgr_state{connections = Conns, owners = Owners}
) ->
    case maps:get(Owner, Owners, undefined) of
        undefined ->
            skip;
        {Ref, _Conn} ->
            _ = erlang:demonitor(Ref, [flush])
    end,
    {
        noreply,
        State#epg_pool_mgr_state{
            connections = unique_queue_push(Connection, Conns),
            owners = maps:without([Owner], Owners)
        }
    };
handle_cast(_Request, State = #epg_pool_mgr_state{}) ->
    {noreply, State}.

handle_info(
    {'DOWN', _MonitorRef, process, Pid, _Info},
    State = #epg_pool_mgr_state{connections = Conns, workers = Workers, monitors = Monitors}
) when erlang:is_map_key(Pid, Workers) ->
    Connection = maps:get(Pid, Workers),
    NewConns = queue:delete(Connection, Conns),
    NewWorkers = maps:without([Pid], Workers),
    NewMonitors = demonitor_and_close(Connection, Monitors),
    {
        noreply,
        State#epg_pool_mgr_state{connections = NewConns, workers = NewWorkers, monitors = NewMonitors}
    };
handle_info(
    {'DOWN', _MonitorRef, process, Pid, _Info},
    State = #epg_pool_mgr_state{connections = Conns, owners = Owners, monitors = Monitors}
) when erlang:is_map_key(Pid, Owners) ->
    {_Ref, Connection} = maps:get(Pid, Owners),
    NewConns = queue:delete(Connection, Conns),
    NewOwners = maps:without([Pid], Owners),
    NewMonitors = demonitor_and_close(Connection, Monitors),
    {
        noreply,
        State#epg_pool_mgr_state{connections = NewConns, owners = NewOwners, monitors = NewMonitors}
    };
%%
handle_info(
    {'DOWN', _MonitorRef, process, Pid, _Info},
    State = #epg_pool_mgr_state{connections = Conns, monitors = Monitors}
) when erlang:is_map_key(Pid, Monitors) ->
    NewConns = queue:delete(Pid, Conns),
    NewMonitors = demonitor_and_close(Pid, Monitors),
    {
        noreply,
        State#epg_pool_mgr_state{connections = NewConns, monitors = NewMonitors}
    };

handle_info(_Info, State = #epg_pool_mgr_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #epg_pool_mgr_state{}) ->
    ok.

code_change(_OldVsn, State = #epg_pool_mgr_state{}, _Extra) ->
    {ok, State}.

%%

start_workers(Pool, Size) ->
    WorkerSup = reg_name(Pool, "_pool_wrk_sup"),
    lists:foreach(fun(N) ->
        supervisor:start_child(WorkerSup, [N])
    end, lists:seq(1, Size)).

reg_name(Name, Postfix) ->
    list_to_atom(atom_to_list(Name) ++ Postfix).

demonitor_and_close(Connection, Monitors) when erlang:is_map_key(Connection, Monitors) ->
    {MRef, NewMonitors} = maps:take(Connection, Monitors),
    _ = catch erlang:demonitor(MRef),
    _ = catch epgsql:close(Connection),
    NewMonitors;
demonitor_and_close(Connection, Monitors) ->
    _ = catch epgsql:close(Connection),
    Monitors.

unique_queue_push(Item, Q) ->
    case queue:member(Item, Q) of
        true -> Q;
        false -> queue:in(Item, Q)
    end.
