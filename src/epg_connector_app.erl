%%%-------------------------------------------------------------------
%% @doc epg_connector public API
%% @end
%%%-------------------------------------------------------------------

-module(epg_connector_app).

-behaviour(application).

-define(VAULT_TOKEN_PATH, "/var/run/secrets/kubernetes.io/serviceaccount/token").
-define(VAULT_ROLE, "api-key-mgmt-v2").
-define(VAULT_KEY_PG_CREDS, "api-key-mgmt-v2/pg_creds").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    Databases = application:get_env(epg_connector, databases, #{}),
    ok = maybe_set_secrets(Databases),
    Pools = application:get_env(epg_connector, pools, #{}),
    ok = start_pools(Pools, Databases),
    epg_connector_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

start_pools(Pools, Databases) ->
    maps:fold(
        fun(PoolName, Opts, _Acc) ->
            #{
                database := DB,
                size := Size
            } = Opts,
            DbParams = maps:get(DB, Databases),
            {ok, _} = epgsql_pool:start(PoolName, Size, Size, DbParams),
            ok
        end,
        ok,
        Pools
    ).

maybe_set_secrets(Databases) ->
    TokenPath = application:get_env(epg_connector, vault_token_path, ?VAULT_TOKEN_PATH),
    try vault_client_auth(TokenPath) of
        ok ->
            Key = unicode:characters_to_binary(
                application:get_env(epg_connector, vault_key_pg_creds, ?VAULT_KEY_PG_CREDS)
            ),
            set_secrets(canal:read(Key), Databases);
        Error ->
            logger:error("can`t auth vault client with error: ~p", [Error]),
            skip
    catch
        _:_ ->
            logger:error("catch exception when auth vault client"),
            skip
    end,
    ok.

vault_client_auth(TokenPath) ->
    case read_maybe_linked_file(TokenPath) of
        {ok, Token} ->
            Role = unicode:characters_to_binary(application:get_env(epg_connector, vault_role, ?VAULT_ROLE)),
            canal:auth({kubernetes, Role, Token});
        Error ->
            Error
    end.

read_maybe_linked_file(MaybeLinkName) ->
    case file:read_link(MaybeLinkName) of
        {error, enoent} = Result ->
            Result;
        {error, einval} ->
            file:read_file(MaybeLinkName);
        {ok, Filename} ->
            file:read_file(maybe_expand_relative(MaybeLinkName, Filename))
    end.

maybe_expand_relative(BaseFilename, Filename) ->
    filename:absname_join(filename:dirname(BaseFilename), Filename).

set_secrets({ok, #{<<"pg_creds">> := #{<<"pg_user">> := PgUser, <<"pg_password">> := PgPassword}}}, Databases) ->
    logger:info("postgres credentials successfuly read from vault (as json)"),
    NewDbConfig = maps:fold(fun(DbName, ConnOpts, Acc) ->
        Acc#{
            DbName => ConnOpts#{
                username => unicode:characters_to_list(PgUser),
                password => unicode:characters_to_list(PgPassword)
            }
        }
    end, #{}, Databases),
    application:set_env(epg_connector, databases, NewDbConfig),
    ok;
set_secrets({ok, #{<<"pg_creds">> := PgCreds}}, Databases) ->
    logger:info("postgres credentials successfuly read from vault (as string)"),
    set_secrets({ok, #{<<"pg_creds">> => jsx:decode(PgCreds, [return_maps])}}, Databases);
set_secrets(Error, _Databases) ->
    logger:error("can`t read postgres credentials from vault with error: ~p", [Error]),
    skip.

