-module(epg_pool).

-export([query/2]).
-export([query/3]).
-export([query/4]).
-export([transaction/2]).
-export([transaction/3]).
-export([with/2]).
-export([with/3]).

-define(DEFAULT_CHECKOUT_TIMEOUT, 5000).
-define(PROTECT_TIMEOUT, 10).

query(Pool, Stmt) when is_atom(Pool)->
    query(Pool, Stmt, []);
query(Conn, Stmt) when is_pid(Conn) ->
    epgsql:equery(Conn, Stmt).

query(Pool, Stmt, Params) when is_atom(Pool) ->
    query(get_connection(Pool), Pool, Stmt, Params);
query(Conn, Stmt, Params) when is_pid(Conn) ->
    epgsql:equery(Conn, Stmt, Params).

query({error, _} = Err, _Pool, _Stmt, _Params) ->
    Err;
query(Conn, Pool, Stmt, Params) when is_pid(Conn) ->
    Result = epgsql:equery(Conn, Stmt, Params),
    ok = epg_pool_mgr:checkin(Pool, self(), Conn),
    Result.

transaction(Pool, Fun) when is_atom(Pool) ->
    transaction(get_connection(Pool), Pool, Fun);
transaction(Conn, Fun) when is_pid(Conn) ->
    epgsql:with_transaction(Conn, Fun).

transaction({error, _} = Err, _Pool, _Fun) ->
    Err;
transaction(Conn, Pool, Fun) when is_pid(Conn) ->
    Result = epgsql:with_transaction(Conn, Fun),
    ok = epg_pool_mgr:checkin(Pool, self(), Conn),
    Result.

with(Pool, Fun) when is_atom(Pool) ->
    with(get_connection(Pool), Pool, Fun);
with(Conn, Fun) when is_pid(Conn) ->
    Fun(Conn).

with({error, _} = Err, _Pool, _F) ->
    Err;
with(Conn, Pool, Fun) when is_pid(Conn) ->
    Result = Fun(Conn),
    ok = epg_pool_mgr:checkin(Pool, self(), Conn),
    Result.
%%

get_connection(Pool) ->
    Timeout = application:get_env(epg_connector, checkout_timeout, ?DEFAULT_CHECKOUT_TIMEOUT),
    get_connection(Pool, Timeout).

get_connection(Pool, Timeout) ->
    case application:get_env(epg_connector, async, enabled) of
        enabled ->
            get_connection_async(Pool, Timeout);
        _ ->
            get_connection_sync(Pool, erlang:system_time(millisecond) + Timeout)
    end.

get_connection_async(Pool, Timeout) ->
    TimeoutWithProtection = Timeout + ?PROTECT_TIMEOUT,
    Ref = make_ref(),
    epg_pool_mgr:checkout_async(Pool, {self(), Timeout, Ref}),
    receive
        {Ref, {error, _} = Error} ->
            Error;
        {Ref, Connection} ->
            Connection
    after
        TimeoutWithProtection ->
            logger:error("pg pool ~p checkout timeout", [Pool]),
            {error, overload}
    end.

get_connection_sync(Pool, Deadline) ->
    Now = erlang:system_time(millisecond),
    case epg_pool_mgr:checkout(Pool) of
        empty when Now < Deadline ->
            logger:warning("pg pool ~p empty", [Pool]),
            timer:sleep(100),
            get_connection_sync(Pool, Deadline);
        empty ->
            logger:error("pg pool ~p checkout timeout", [Pool]),
            {error, overload};
        {error, Reason} = Err ->
            logger:error("pg pool ~p error: ~p", [Reason]),
            Err;
        Connection when is_pid(Connection) ->
            Connection
    end.
