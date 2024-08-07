# epg_connector

[![Erlang/OTP Version](https://img.shields.io/badge/Erlang%2FOTP-21%2B-blue.svg)](http://www.erlang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`epg_connector` is a robust Erlang application that provides a connection pool for PostgreSQL databases with seamless Vault integration for secret management. It simplifies database connection handling and credential management in Erlang applications, making it easier to build scalable and secure database-driven systems.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
  - [Database Configuration](#database-configuration)
  - [Pool Configuration](#pool-configuration)
  - [Vault Configuration](#vault-configuration)
- [Usage](#usage)
- [Vault Integration](#vault-integration)
- [Error Handling and Logging](#error-handling-and-logging)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## Features

- 🚀 Efficient PostgreSQL connection pooling
- 🔒 Seamless Vault integration for secure secret management
- ⚙️ Highly configurable database and pool settings
- 🔑 Automatic credential retrieval from Vault
- 📊 Built-in error logging and handling
- 🔌 Easy integration with existing Erlang/OTP applications

## Prerequisites

Before you begin, ensure you have the following installed:

- Erlang/OTP 21 or later
- Rebar3 (build tool for Erlang)
- PostgreSQL database
- (Optional) HashiCorp Vault for secret management

## Installation

Add `epg_connector` to your `rebar.config` dependencies:

```erlang
{deps, [
    {epg_connector, {git, "https://github.com/your-repo/epg_connector.git", {tag, "1.0.0"}}}
]}.
```

Then run:

```bash
$ rebar3 get-deps
$ rebar3 compile
```

## Configuration

### Database Configuration

Configure your databases in your `sys.config` file:

```erlang
{databases, #{
    default_db => #{
        host => "127.0.0.1",
        port => 5432,
        database => "db_name",
        username => "postgres",
        password => "postgres"
    },
    another_db => #{
        host => "db.example.com",
        port => 5432,
        database => "another_db",
        username => "user",
        password => "pass"
    }
}}
```

### Pool Configuration

Set up your connection pools:

```erlang
{pools, #{
    default_pool => #{
        database => default_db,
        size => 10
    },
    read_only_pool => #{
        database => another_db,
        size => 5
    }
}}
```

### Vault Configuration

If using Vault for secret management, configure the following:

```erlang
{vault_token_path, "/var/run/secrets/kubernetes.io/serviceaccount/token"},
{vault_role, "epg_connector"},
{vault_key_pg_creds, "epg_connector/pg_creds"}
```

## Usage

1. Ensure `epg_connector` is started with your application:

   ```erlang
   {applications, [kernel, stdlib, epg_connector]}.
   ```

2. Use the connection pool in your code:

   ```erlang
   -module(my_db_module).
   -export([get_user/1]).

   get_user(UserId) ->
       epgsql_pool:with(default_pool, fun(C) ->
           {ok, _, [{Name, Email}]} = epgsql:equery(C, "SELECT name, email FROM users WHERE id = $1", [UserId]),
           {Name, Email}
       end).
   ```

## Vault Integration

When Vault is configured, `epg_connector` automatically attempts to retrieve database credentials on startup. This process involves:

1. Reading the Vault token from the specified path
2. Authenticating with Vault using the configured role
3. Fetching the database credentials from the specified Vault key
4. Updating the database configuration with the retrieved credentials

Ensure your Vault is properly set up and the application has the necessary permissions to access the secrets.

## Error Handling and Logging

`epg_connector` includes comprehensive error handling and logging:

- Vault authentication failures are logged with detailed error messages
- Credential retrieval issues are captured and reported
- Database connection errors are logged for easy troubleshooting

Monitor your application logs for any configuration or connection issues. Example log message:

```
2024-07-24 19:15:30.123 [error] <0.123.0> can't auth vault client with error: {error, permission_denied}
```

## Contributing

We welcome contributions to `epg_connector`! Here's how you can help:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [epgsql](https://github.com/epgsql/epgsql) - Erlang PostgreSQL client
- [epgsql_pool](https://github.com/wgnet/epgsql_pool) - Connection pool for epgsql
- [canal](https://github.com/valitydev/canal) - Erlang Vault client

---

For more information or support, please [open an issue](https://github.com/your-repo/epg_connector/issues/new) or contact the maintainers.
