-module(epg_pool).

-export([query/2]).
-export([query/3]).
-export([query/4]).
-export([transaction/2]).
-export([transaction/3]).
-export([with_transaction/3]).
-export([with/2]).
-export([with/3]).

%% NOTE With default transaction opts as in epgsql:with_transaction/2
-define(DEFAULT_TRANSACTION_OPTS, [{reraise, false}]).

-define(CHECKOUT_TIMEOUT, 5000).

query(Pool, Stmt) when is_atom(Pool) ->
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

-spec transaction(epgsql:connection() | atom(), fun((epgsql:connection()) -> Reply)) ->
    Reply | {rollback, any()} | no_return()
when
    Reply :: any().
transaction(ConnOrPool, Fun) ->
    with_transaction(ConnOrPool, Fun, ?DEFAULT_TRANSACTION_OPTS).

-spec transaction(epgsql:connection(), atom(), fun((epgsql:connection()) -> Reply)) ->
    Reply | {rollback, any()} | no_return()
when
    Reply :: any().
transaction(Conn, Pool, Fun) ->
    with_transaction_(Conn, Pool, Fun, ?DEFAULT_TRANSACTION_OPTS).

-spec with_transaction(
    epgsql:connection() | atom(), fun((epgsql:connection()) -> Reply), epgsql:transaction_opts()
) ->
    Reply | {rollback, any()} | no_return()
when
    Reply :: any().
with_transaction(Pool, Fun, Opts) when is_atom(Pool) andalso is_function(Fun) ->
    with_transaction_(get_connection(Pool), Pool, Fun, Opts);
with_transaction(Conn, Fun, Opts) when is_pid(Conn) andalso is_function(Fun) ->
    epgsql:with_transaction(Conn, Fun, Opts).

with_transaction_({error, _} = Err, _Pool, _Fun, _Opts) ->
    Err;
with_transaction_(Conn, Pool, Fun, Opts) when is_pid(Conn) ->
    Result = epgsql:with_transaction(Conn, Fun, Opts),
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
    get_connection(Pool, erlang:system_time(millisecond) + ?CHECKOUT_TIMEOUT).

get_connection(Pool, Deadline) ->
    Now = erlang:system_time(millisecond),
    case epg_pool_mgr:checkout(Pool) of
        empty when Now < Deadline ->
            logger:warning("pg pool ~p empty", [Pool]),
            timer:sleep(100),
            get_connection(Pool, Deadline);
        empty ->
            logger:error("pg pool ~p checkout timeout", [Pool]),
            {error, overload};
        {error, Reason} = Err ->
            logger:error("pg pool ~p error: ~p", [Reason]),
            Err;
        Connection when is_pid(Connection) ->
            Connection
    end.
