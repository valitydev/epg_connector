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
-export([unwrap_secret/1]).

-type db_ref() :: atom().
-type db_opts() :: #{
    host => string(),
    port => pos_integer(),
    database => string(),
    username => string(),
    password => string() | function()
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
    _Databases = wrap_secrets(Databases0),
    epg_connector_sup:start_link().

stop(_State) ->
    ok.

-spec start_pools(db_configs()) -> supervisor:startchild_ret().
start_pools(#{databases := Databases, pools := Pools} = _DbConf) ->
    ChildSpec = epg_connector_sup:pool_specs(Pools, Databases),
    supervisor:start_child(epg_connector_sup, ChildSpec).

-spec unwrap_secret(db_opts()) -> db_opts().
unwrap_secret(#{password := WrappedPass} = DbOpts) when is_function(WrappedPass) ->
    DbOpts#{password => WrappedPass()};
unwrap_secret(DbOpts) ->
    DbOpts.

%% internal functions

-spec wrap_secret(db_opts()) -> db_opts().
wrap_secret(#{password := Pass} = DbOpts) when is_list(Pass) ->
    DbOpts#{password => fun() -> Pass end};
wrap_secret(DbOpts) ->
    DbOpts.

maybe_start_canal([]) ->
    ok;
maybe_start_canal(_Env) ->
    _ = application:ensure_all_started(canal).

wrap_secrets(Databases) ->
    TokenPath = application:get_env(epg_connector, vault_token_path, ?VAULT_TOKEN_PATH),
    VaultAuthResult = catch vault_client_auth(TokenPath),
    update_env(VaultAuthResult, Databases).

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

update_env(VaultAuthResult, Databases) ->
    DbConfig = update_db_config(VaultAuthResult, Databases),
    ok = application:set_env(epg_connector, databases, DbConfig),
    DbConfig.

update_db_config(VaultAuthResult, Databases) ->
    maps:fold(
        fun(DbName, ConnOpts0, Acc) ->
            ConnOpts = wrap_secret(ConnOpts0),
            case read_secret(VaultAuthResult, DbName) of
                {ok, {PgUser, PgPassword}} ->
                    Acc#{
                        DbName => ConnOpts#{
                            username => unicode:characters_to_list(PgUser),
                            password => fun() -> unicode:characters_to_list(PgPassword) end
                        }
                    };
                {error, _Error} ->
                    Acc#{DbName => ConnOpts}
            end
        end,
        #{},
        Databases
    ).

read_secret(ok, DbName) ->
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
    end;
read_secret({error, _} = Error, _DbName) ->
    logger:error("can`t auth vault client with error: ~p", [Error]),
    {error, {canal, auth_error}};
read_secret(_Exception, _DbName) ->
    logger:error("catch exception when auth vault client"),
    {error, {canal, auth_error}}.
