-module(epg_pool_mgr).

-behaviour(gen_server).

-export([start_link/3]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([checkout/1]).
-export([checkin/3]).
-export([add/3]).
-export([remove/3]).

-record(epg_pool_mgr_state, {
    pool, params, size, connections, workers, owners, monitors, ephemerals
}).

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

%%%

start_link(PoolName, DbParams, Size) ->
    RegName = reg_name(PoolName, "_pool_mgr"),
    gen_server:start_link({local, RegName}, ?MODULE, [PoolName, DbParams, Size], []).

init([PoolName, DbParams, Size]) ->
    erlang:process_flag(trap_exit, true),
    _ = start_workers(PoolName, Size),
    {ok, #epg_pool_mgr_state{
        pool = PoolName,
        params = DbParams,
        size = range(Size),
        connections = queue:new(),
        workers = #{},
        owners = #{},
        monitors = #{},
        ephemerals = #{}
    }}.

%% nested checkout
handle_call(
    checkout,
    {Pid, _},
    State = #epg_pool_mgr_state{
        connections = Conns, monitors = Monitors, owners = Owners, ephemerals = Ephemerals
    }
) when erlang:is_map_key(Pid, Owners) ->
    {{Ref, Conn}, NewOwners} = maps:take(Pid, Owners),
    _ = catch erlang:demonitor(Ref),
    NewConns = queue:delete(Conn, Conns),
    {NewMonitors, NewEphemerals} = demonitor_and_close(Conn, Monitors, Ephemerals),
    {
        reply,
        {error, nested_checkout},
        State#epg_pool_mgr_state{
            connections = NewConns,
            monitors = NewMonitors,
            owners = NewOwners,
            ephemerals = NewEphemerals
        }
    };
handle_call(
    checkout,
    {Pid, _Ref},
    State = #epg_pool_mgr_state{connections = Conns, owners = Owners}
) ->
    {Result, NewConns} = queue:out(Conns),
    {State1, Response} =
        case Result of
            {value, Connection} ->
                Ref = erlang:monitor(process, Pid),
                {State#epg_pool_mgr_state{owners = Owners#{Pid => {Ref, Connection}}}, Connection};
            empty ->
                %% try ephemeral connection
                maybe_new_connection(Pid, State)
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
    State = #epg_pool_mgr_state{
        connections = Conns, workers = Workers, monitors = Monitors, ephemerals = Ephemerals
    }
) ->
    NewConns = queue:delete(Connection, Conns),
    NewWorkers = maps:without([Worker], Workers),
    {NewMonitors, NewEphemerals} = demonitor_and_close(Connection, Monitors, Ephemerals),
    {
        noreply,
        State#epg_pool_mgr_state{
            connections = NewConns,
            workers = NewWorkers,
            monitors = NewMonitors,
            ephemerals = NewEphemerals
        }
    };
%% stable connection checkin
handle_cast(
    {checkin, Owner, Connection},
    State = #epg_pool_mgr_state{connections = Conns, owners = Owners, monitors = Monitors}
) when erlang:is_map_key(Connection, Monitors) ->
    _ = demonitor_owner(Owner, Owners),
    {
        noreply,
        State#epg_pool_mgr_state{
            connections = unique_queue_push(Connection, Conns),
            owners = maps:without([Owner], Owners)
        }
    };
%% ephemeral connection checkin
handle_cast(
    {checkin, Owner, Connection},
    State = #epg_pool_mgr_state{owners = Owners, ephemerals = Ephemerals, monitors = Monitors}
) when erlang:is_map_key(Connection, Ephemerals) ->
    _ = demonitor_owner(Owner, Owners),
    {NewMonitors, NewEphemerals} = demonitor_and_close(Connection, Monitors, Ephemerals),
    {
        noreply,
        State#epg_pool_mgr_state{
            owners = maps:without([Owner], Owners),
            monitors = NewMonitors,
            ephemerals = NewEphemerals
        }
    };
handle_cast(_Request, State = #epg_pool_mgr_state{}) ->
    {noreply, State}.

%% worker down
handle_info(
    {'DOWN', _MonitorRef, process, Pid, _Info},
    State = #epg_pool_mgr_state{
        connections = Conns,
        owners = Owners,
        workers = Workers,
        monitors = Monitors,
        ephemerals = Ephemerals
    }
) when erlang:is_map_key(Pid, Workers) ->
    Connection = maps:get(Pid, Workers),
    NewConns = queue:delete(Connection, Conns),
    NewWorkers = maps:without([Pid], Workers),
    NewOwners = cleanup_owners(Connection, Owners),
    {NewMonitors, NewEphemerals} = demonitor_and_close(Connection, Monitors, Ephemerals),
    {
        noreply,
        State#epg_pool_mgr_state{
            connections = NewConns,
            owners = NewOwners,
            workers = NewWorkers,
            monitors = NewMonitors,
            ephemerals = NewEphemerals
        }
    };
%% owner down
handle_info(
    {'DOWN', _MonitorRef, process, Pid, _Info},
    State = #epg_pool_mgr_state{
        connections = Conns,
        owners = Owners,
        monitors = Monitors,
        ephemerals = Ephemerals
    }
) when erlang:is_map_key(Pid, Owners) ->
    {_Ref, Connection} = maps:get(Pid, Owners),
    NewConns = queue:delete(Connection, Conns),
    NewOwners = maps:without([Pid], Owners),
    {NewMonitors, NewEphemerals} = demonitor_and_close(Connection, Monitors, Ephemerals),
    {
        noreply,
        State#epg_pool_mgr_state{
            connections = NewConns,
            owners = NewOwners,
            monitors = NewMonitors,
            ephemerals = NewEphemerals
        }
    };
%% stable connection down
handle_info(
    {'DOWN', _MonitorRef, process, Pid, _Info},
    State = #epg_pool_mgr_state{
        connections = Conns,
        owners = Owners,
        monitors = Monitors,
        ephemerals = Ephemerals
    }
) when erlang:is_map_key(Pid, Monitors) ->
    NewConns = queue:delete(Pid, Conns),
    NewOwners = cleanup_owners(Pid, Owners),
    {NewMonitors, NewEphemerals} = demonitor_and_close(Pid, Monitors, Ephemerals),
    {
        noreply,
        State#epg_pool_mgr_state{
            connections = NewConns,
            owners = NewOwners,
            monitors = NewMonitors,
            ephemerals = NewEphemerals
        }
    };
%% ephemeral connection down
handle_info(
    {'DOWN', _MonitorRef, process, Pid, _Info},
    State = #epg_pool_mgr_state{
        owners = Owners,
        monitors = Monitors,
        ephemerals = Ephemerals
    }
) when erlang:is_map_key(Pid, Ephemerals) ->
    NewOwners = cleanup_owners(Pid, Owners),
    {NewMonitors, NewEphemerals} = demonitor_and_close(Pid, Monitors, Ephemerals),
    {
        noreply,
        State#epg_pool_mgr_state{
            owners = NewOwners, monitors = NewMonitors, ephemerals = NewEphemerals
        }
    };
handle_info(_Info, State = #epg_pool_mgr_state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #epg_pool_mgr_state{}) ->
    ok.

code_change(_OldVsn, State = #epg_pool_mgr_state{}, _Extra) ->
    {ok, State}.

%%

start_workers(Pool, {Min, _Max}) ->
    start_workers(Pool, Min);
start_workers(Pool, Size) when is_integer(Size) ->
    WorkerSup = reg_name(Pool, "_pool_wrk_sup"),
    lists:foreach(
        fun(N) ->
            supervisor:start_child(WorkerSup, [N])
        end,
        lists:seq(1, Size)
    ).

reg_name(Name, Postfix) ->
    list_to_atom(atom_to_list(Name) ++ Postfix).

demonitor_and_close(Connection, Monitors, Ephemerals) when
    erlang:is_map_key(Connection, Monitors)
->
    {MRef, NewMonitors} = maps:take(Connection, Monitors),
    _ = catch erlang:demonitor(MRef),
    _ = catch epgsql:close(Connection),
    {NewMonitors, Ephemerals};
demonitor_and_close(Connection, Monitors, Ephemerals) when
    erlang:is_map_key(Connection, Ephemerals)
->
    {MRef, NewEphemerals} = maps:take(Connection, Ephemerals),
    _ = catch erlang:demonitor(MRef),
    _ = catch epgsql:close(Connection),
    {Monitors, NewEphemerals};
demonitor_and_close(Connection, Monitors, Ephemerals) ->
    _ = catch epgsql:close(Connection),
    {Monitors, Ephemerals}.

demonitor_owner(Owner, Owners) ->
    case maps:get(Owner, Owners, undefined) of
        undefined ->
            skip;
        {Ref, _Conn} ->
            _ = erlang:demonitor(Ref)
    end.

cleanup_owners(Connection, Owners) ->
    SearchOwner = maps:fold(
        fun
            (Own, {Ref, Conn}, _Acc) when Conn =:= Connection ->
                {Own, Ref, Conn};
            (_, _, Acc) ->
                Acc
        end,
        not_found,
        Owners
    ),
    case SearchOwner of
        {Owner, OwnRef, Connection} ->
            _ = catch erlang:demonitor(OwnRef),
            maps:without([Owner], Owners);
        not_found ->
            Owners
    end.

unique_queue_push(Item, Q) ->
    case queue:member(Item, Q) of
        true -> Q;
        false -> queue:in(Item, Q)
    end.

range({_Min, _Max} = Range) ->
    Range;
range(Size) when is_integer(Size) ->
    {Size, Size}.

maybe_new_connection(
    Owner,
    #epg_pool_mgr_state{
        size = {Min, Max},
        ephemerals = Ephemerals
    } = State
) ->
    AllConnectionsCount = Min + maps:size(Ephemerals),
    case AllConnectionsCount < Max of
        true ->
            connect_ephemeral(Owner, State);
        false ->
            {State, empty}
    end.

connect_ephemeral(
    Owner,
    #epg_pool_mgr_state{
        pool = Pool,
        params = #{database := DB} = Params,
        owners = Owners,
        ephemerals = Ephemerals
    } = State
) ->
    try epgsql:connect(Params) of
        {ok, Connection} ->
            logger:info("db dynamic connection established. pool: ~p. database: ~p", [Pool, DB]),
            OwnerRef = erlang:monitor(process, Owner),
            ConnRef = erlang:monitor(process, Connection),
            NewOwners = Owners#{Owner => {OwnerRef, Connection}},
            NewEphemerals = Ephemerals#{Connection => ConnRef},
            {State#epg_pool_mgr_state{owners = NewOwners, ephemerals = NewEphemerals}, Connection};
        {error, Reason} ->
            logger:warning(
                "db can`t establish dynamic connection. pool: ~p. database: ~p. error: ~p",
                [Pool, DB, Reason]
            ),
            {State, empty}
    catch
        _Class:Reason:Trace ->
            logger:error(
                "db can`t establish dynamic connection. pool: ~p. database: ~p. error: ~p. trace: ~p",
                [Pool, DB, Reason, Trace]
            ),
            {State, empty}
    end.
