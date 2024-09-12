-module(epg_pool).

-export([query/2]).
-export([query/3]).
-export([query/4]).
-export([transaction/2]).
-export([transaction/3]).
-export([with/2]).
-export([with/3]).

query(Pool, Stmt) when is_atom(Pool)->
    query(Pool, Stmt, []);
query(Conn, Stmt) when is_pid(Conn) ->
    epgsql:equery(Conn, Stmt).

query(Pool, Stmt, Params) when is_atom(Pool) ->
    query(epg_pool_mgr:checkout(Pool), Pool, Stmt, Params);
query(Conn, Stmt, Params) when is_pid(Conn) ->
    epgsql:equery(Conn, Stmt, Params).

query(empty, _Pool, _Stmt, _Params) ->
    {error, overload};
query({error, _} = Err, _Pool, _Stmt, _Params) ->
    Err;
query(Conn, Pool, Stmt, Params) when is_pid(Conn) ->
    Result = epgsql:equery(Conn, Stmt, Params),
    _ = epg_pool_mgr:checkin(Pool, self(), Conn),
    Result.

transaction(Pool, Fun) when is_atom(Pool) ->
    transaction(epg_pool_mgr:checkout(Pool), Pool, Fun);
transaction(Conn, Fun) when is_pid(Conn) ->
    epgsql:with_transaction(Conn, Fun).

transaction(empty, _Pool, _Fun) ->
    {error, overload};
transaction({error, _} = Err, _Pool, _Fun) ->
    Err;
transaction(Conn, Pool, Fun) when is_pid(Conn) ->
    Result = epgsql:with_transaction(Conn, Fun),
    _ = epg_pool_mgr:checkin(Pool, self(), Conn),
    Result.

with(Pool, Fun) when is_atom(Pool) ->
    with(epg_pool_mgr:checkout(Pool), Pool, Fun);
with(Conn, Fun) when is_pid(Conn) ->
    Fun(Conn).

with(empty, _Pool, _F) ->
    {error, overload};
with(Conn, Pool, Fun) when is_pid(Conn) ->
    Result = Fun(Conn),
    _ = epg_pool_mgr:checkin(Pool, self(), Conn),
    Result.
