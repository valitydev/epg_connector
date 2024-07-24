# epg_connector

`epg_connector` is an Erlang application that provides a connection pool for PostgreSQL databases with integrated Vault secret management. It simplifies the process of managing database connections and credentials in Erlang applications.

## Features

- PostgreSQL connection pooling
- Vault integration for secret management
- Configurable database and pool settings
- Automatic credential retrieval from Vault

## Prerequisites

- Erlang/OTP 21 or later
- Rebar3
- PostgreSQL database
- (Optional) Vault for secret management

## Installation

Add `epg_connector` to your `rebar.config` dependencies:

```erlang
{deps, [
    {epg_connector, {git, "https://github.com/your-repo/epg_connector.git", {branch, "master"}}}
]}.
```

## Configuration

### sys.config

Configure your databases, pools, and Vault settings in your `sys.config` file:

```erlang
[
    {epg_connector, [
        {databases, #{
            default_db => #{
                host => "127.0.0.1",
                port => 5432,
                database => "db_name",
                username => "postgres",
                password => "postgres"
            }
        }},
        {pools, #{
            default_pool => #{
                database => default_db,
                size => 10
            }
        }},
        {vault_token_path, "/var/run/secrets/kubernetes.io/serviceaccount/token"},
        {vault_role, "epg_connector"},
        {vault_key_pg_creds, "epg_connector/pg_creds"}
    ]},
    {canal, [
        {url, "http://vault:8200"},
        {engine, kvv2}
    ]}
].
```

## Usage

1. Ensure `epg_connector` is started with your application:

   ```erlang
   {applications, [kernel, stdlib, epg_connector]}.
   ```

2. Use the connection pool in your code:

   ```erlang
   epgsql_pool:with(PoolName, fun(C) ->
       epgsql:equery(C, "SELECT * FROM some_table")
   end).
   ```

## Vault Integration

If Vault is configured, `epg_connector` will attempt to retrieve database credentials from Vault on startup. Ensure your Vault is properly set up and the application has the necessary permissions.

## Error Handling

The application includes error logging for issues with Vault authentication or credential retrieval. Monitor your application logs for any configuration or connection issues.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
