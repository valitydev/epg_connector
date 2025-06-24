%%%-------------------------------------------------------------------
%% @doc epg_connector public API
%% @end
%%%-------------------------------------------------------------------

-module(epg_connector_app).

-behaviour(application).

-define(VAULT_TOKEN_PATH, "/var/run/secrets/kubernetes.io/serviceaccount/token").
-define(VAULT_ROLE, "epg_connector").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    _ = maybe_start_canal(application:get_all_env(canal)),
    Databases0 = application:get_env(epg_connector, databases, #{}),
    _Databases = maybe_set_secrets(Databases0),
    epg_connector_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

maybe_start_canal([]) ->
    ok;
maybe_start_canal(_Env) ->
    _ = application:ensure_all_started(canal).

maybe_set_secrets(Databases) ->
    TokenPath = application:get_env(epg_connector, vault_token_path, ?VAULT_TOKEN_PATH),
    try vault_client_auth(TokenPath) of
        ok ->
            set_secrets(Databases);
        Error ->
            logger:error("can`t auth vault client with error: ~p", [Error]),
            Databases
    catch
        _:_ ->
            logger:error("catch exception when auth vault client"),
            Databases
    end.

vault_client_auth(TokenPath) ->
    case read_maybe_linked_file(TokenPath) of
        {ok, Token} ->
            Role = unicode:characters_to_binary(application:get_env(epg_connector, vault_role, ?VAULT_ROLE)),
            try_auth(Role, Token);
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

try_auth(Role, Token) ->
    try
        canal:auth({kubernetes, Role, Token})
    catch
        _:Error:StackTrace ->
            logger:error("canal auth error: ~p trace: ~p", [Error, StackTrace]),
            {error, {canal, auth_error}}
    end.

set_secrets(Databases) ->
    DbConfig = update_db_config(Databases),
    application:set_env(epg_connector, databases, DbConfig),
    DbConfig.

update_db_config(Databases) ->
    maps:fold(fun(DbName, ConnOpts, Acc) ->
        case read_secret(DbName) of
            {ok, {PgUser, PgPassword}} ->
                Acc#{
                    DbName => ConnOpts#{
                        username => unicode:characters_to_list(PgUser),
                        password => unicode:characters_to_list(PgPassword)
                    }
                };
            {error, _Error} ->
                Acc#{DbName => ConnOpts}
         end
    end, #{}, Databases).

read_secret(DbName) ->
    DefaultKeyPath = "pg_creds/" ++ erlang:atom_to_list(DbName),
    KeyPath = application:get_env(epg_connector, vault_secret_key_path, DefaultKeyPath),
    case canal:read(KeyPath) of
        {ok, #{<<"pg_creds">> := #{<<"pg_user">> := PgUser, <<"pg_password">> := PgPassword}}} ->
            logger:info("postgres credentials successfuly read from vault (as json)"),
            {ok, {unicode:characters_to_list(PgUser), unicode:characters_to_list(PgPassword)}};
        {ok, #{<<"pg_creds">> := PgCreds}} ->
            logger:info("postgres credentials successfuly read from vault (as string)"),
            #{<<"pg_user">> := PgUser, <<"pg_password">> := PgPassword} = jsx:decode(PgCreds, [return_maps]),
            {ok, {unicode:characters_to_list(PgUser), unicode:characters_to_list(PgPassword)}};
        Error ->
            logger:error("can`t read postgres credentials from vault with error: ~p", [Error]),
            {error, Error}
    end.
