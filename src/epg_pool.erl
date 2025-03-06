-module(epg_pool).

-export([query/2]).
-export([query/3]).
-export([query/4]).
-export([transaction/2]).
-export([transaction/3]).
-export([with/2]).
-export([with/3]).

-define(CHECKOUT_TIMEOUT, 5000).

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

%% FIXME Make transaction funcs API with less confusing MFA
transaction(Pool, Opts, Fun) when is_atom(Pool) andalso is_list(Opts) ->
    transaction_(get_connection(Pool), Pool, Fun, Opts);
transaction(Conn, Opts, Fun) when is_pid(Conn) andalso is_list(Opts) ->
    epgsql:with_transaction(Conn, Fun, Opts);
transaction(Conn, Pool, Fun) ->
    %% NOTE With default transaction opts as in epgsql:with_transaction/2
    transaction_(Conn, Pool, Fun, [{reraise, false}]).

transaction_({error, _} = Err, _Pool, _Fun, _Opts) ->
    Err;
transaction_(Conn, Pool, Fun, Opts) when is_pid(Conn) ->
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
