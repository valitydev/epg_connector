%%%-------------------------------------------------------------------
%% @doc epg_connector public API
%% @end
%%%-------------------------------------------------------------------

-module(epg_connector_app).

-behaviour(application).

-define(VAULT_TOKEN_PATH, "/var/run/secrets/kubernetes.io/serviceaccount/token").
-define(VAULT_ROLE, "epg_connector").

-export([start/2, stop/1]).

-export([start_pools/1]).

-type db_ref() :: atom().
-type db_opts() :: #{
    host => string(),
    port => pos_integer(),
    database => string(),
    username => string(),
    password => string()
}.
-type databases() :: #{db_ref() := db_opts()}.
-type pool_name() :: atom().
-type pool_opts() :: #{
    database := db_ref(),
    %% MaxConnections - PermanentConnections = EphemeralConnections
    size := pos_integer() | {PermanentConnections :: pos_integer(), MaxConnections :: pos_integer()}
}.
-type pools() :: #{pool_name() := pool_opts()}.
-type db_configs() :: #{
    databases := databases(),
    pools := pools()
}.

-export_type([databases/0]).
-export_type([pools/0]).
-export_type([db_configs/0]).
-export_type([db_opts/0]).
-export_type([db_ref/0]).

start(_StartType, _StartArgs) ->
    _ = maybe_start_canal(application:get_all_env(canal)),
    Databases0 = application:get_env(epg_connector, databases, #{}),
    _Databases = maybe_set_secrets(Databases0),
    epg_connector_sup:start_link().

stop(_State) ->
    ok.

-spec start_pools(db_configs()) -> supervisor:startchild_ret().
start_pools(#{databases := Databases, pools := Pools} = _DbConf) ->
    ChildSpec = epg_connector_sup:pool_specs(Pools, Databases),
    supervisor:start_child(epg_connector_sup, ChildSpec).

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
            Role = unicode:characters_to_binary(
                application:get_env(epg_connector, vault_role, ?VAULT_ROLE)
            ),
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
        _:_ ->
            {error, {canal, auth_error}}
    end.

set_secrets(Databases) ->
    DbConfig = update_db_config(Databases),
    application:set_env(epg_connector, databases, DbConfig),
    DbConfig.

update_db_config(Databases) ->
    maps:fold(
        fun(DbName, ConnOpts, Acc) ->
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
        end,
        #{},
        Databases
    ).

read_secret(DbName) ->
    DefaultKeyPath = "pg_creds/" ++ erlang:atom_to_list(DbName),
    KeyPaths = application:get_env(epg_connector, vault_secret_key_paths, #{}),
    KeyPath = maps:get(DbName, KeyPaths, DefaultKeyPath),
    case canal:read(KeyPath) of
        {ok, #{<<"pg_creds">> := #{<<"pg_user">> := PgUser, <<"pg_password">> := PgPassword}}} ->
            logger:info("postgres credentials successfuly read from vault (as json)"),
            {ok, {unicode:characters_to_list(PgUser), unicode:characters_to_list(PgPassword)}};
        {ok, #{<<"pg_creds">> := PgCreds}} ->
            logger:info("postgres credentials successfuly read from vault (as string)"),
            #{<<"pg_user">> := PgUser, <<"pg_password">> := PgPassword} = jsx:decode(PgCreds, [
                return_maps
            ]),
            {ok, {unicode:characters_to_list(PgUser), unicode:characters_to_list(PgPassword)}};
        Error ->
            logger:error("can`t read postgres credentials from vault with error: ~p", [Error]),
            {error, Error}
    end.
