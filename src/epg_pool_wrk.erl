-module(epg_pool_wrk).

-behaviour(gen_server).

-export([start_link/4]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    handle_continue/2,
    code_change/3
]).

%% API

-record(epg_pool_wrk_state, {pool, params, size, connection, monitor}).

start_link(PoolName, DbParams, Size, _N) ->
    gen_server:start_link(?MODULE, [PoolName, DbParams, Size], []).

init([PoolName, DbParams, Size]) ->
    erlang:process_flag(trap_exit, true),
    State0 = #epg_pool_wrk_state{pool = PoolName, params = DbParams, size = Size},
    {ok, connect(State0)}.

handle_continue(init, State) ->
    {noreply, connect(State)}.

handle_call(_Request, _From, #epg_pool_wrk_state{} = State) ->
    {reply, ok, State}.

handle_cast(_Request, #epg_pool_wrk_state{} = State) ->
    {noreply, State}.

handle_info(
    {'EXIT', Pid, _Info},
    #epg_pool_wrk_state{pool = Pool, connection = Pid, params = #{database := DB}} = State
) ->
    %epg_pool_mgr:remove(Pool, self(), Pid),
    logger:error("db connection lost. pool: ~p. database: ~p", [Pool, DB]),
    reconnect_timer(50),
    {noreply, State#epg_pool_wrk_state{connection = undefined, monitor = undefined}};
handle_info({timeout, _Ref, reconnect}, #epg_pool_wrk_state{} = State) ->
    {noreply, connect(State)};
handle_info(_Info, #epg_pool_wrk_state{} = State) ->
    {noreply, State}.

terminate(_Reason, #epg_pool_wrk_state{} = _State) ->
    ok.

code_change(_OldVsn, #epg_pool_wrk_state{} = State, _Extra) ->
    {ok, State}.
%%

reconnect_timer() ->
    reconnect_timer(5000).

reconnect_timer(Timeout) ->
    erlang:start_timer(Timeout, self(), reconnect).

connect(#epg_pool_wrk_state{pool = Pool, params = #{database := DB} = Params} = State) ->
    try epgsql:connect(Params) of
        {ok, Connection} ->
            epg_pool_mgr:add(Pool, self(), Connection),
            logger:info("db connection established. pool: ~p. database: ~p", [Pool, DB]),
            State#epg_pool_wrk_state{connection = Connection};
        {error, _Reason} ->
            reconnect_timer(),
            State
    catch
        _:_ ->
            reconnect_timer(),
            State
    end.
