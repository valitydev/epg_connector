# EPG Connector

[![Erlang/OTP Version](https://img.shields.io/badge/Erlang%2FOTP-21%2B-blue.svg)](http://www.erlang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`epg_connector` is a high-performance Erlang application that provides PostgreSQL connection pooling and logical replication support. It offers a robust foundation for building scalable database-driven applications with real-time data synchronization capabilities.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
  - [Database and Pool Configuration](#database-and-pool-configuration)
- [Usage](#usage)
  - [Connection Pooling](#connection-pooling)
  - [Logical Replication](#logical-replication)
- [Logical Replication Protocol](#logical-replication-protocol)
- [Data Types Support](#data-types-support)
- [Examples](#examples)
- [License](#license)

## Features

- 🚀 **High-performance PostgreSQL connection pooling** - Efficient connection management with configurable pool sizes
- 📡 **PostgreSQL Logical Replication** - Real-time data streaming using PostgreSQL's logical replication protocol
- 🔄 **pgoutput Protocol Support** - Built-in decoder for PostgreSQL's native logical replication output plugin
- 📊 **Comprehensive Data Type Support** - Handles all PostgreSQL data types including arrays, JSON, timestamps, and custom types
- ⚙️ **Highly Configurable** - Flexible configuration for pools, databases, and replication settings
- 🔌 **Easy Integration** - Simple API for existing Erlang/OTP applications
- 🛡️ **Robust Error Handling** - Comprehensive error handling and logging for production environments

## Prerequisites

Before you begin, ensure you have the following:

- Erlang/OTP 21 or later
- Rebar3 (build tool for Erlang)
- PostgreSQL 10+ with logical replication enabled
- Replication slot configured in PostgreSQL (for logical replication)

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

### Database and Pool Configuration

Configure your databases and connection pools in your `sys.config` file:

```erlang
{epg_connector, [
    {databases, #{
        main_db => #{
            host => "127.0.0.1",
            port => 5432,
            database => "myapp_production",
            username => "postgres",
            password => "postgres"
        },
        read_db => #{
            host => "replica.example.com",
            port => 5432,
            database => "myapp_production",
            username => "readonly_user",
            password => "readonly_pass"
        }
    }},
    {pools, #{
        main_pool => #{
            database => main_db,
            size => {20, 50}
        },
        readonly_pool => #{
            database => read_db,
            size => 10
        }
    }}
]}.
```

## Usage

### Connection Pooling

1. Start the application:

  via *_app.src file

   ```erlang
   {applications, [kernel, stdlib, epgsql, epg_connector]}.
   ```

2. Use connection pools in your code:

   ```erlang
   -module(user_service).
   -export([get_user/1, create_user/2]).

   get_user(UserId) ->
       epg_pool:with(readonly_pool, fun(Connection) ->
           Query = "SELECT id, name, email FROM users WHERE id = $1",
           case epgsql:equery(Connection, Query, [UserId]) of
               {ok, _Columns, [{Id, Name, Email}]} ->
                   {ok, #{id => Id, name => Name, email => Email}};
               {ok, _Columns, []} ->
                   {error, not_found}
           end
       end).

   create_user(Name, Email) ->
       epg_pool:query(
           main_pool,
           "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
           [Name, Email]
       ).
   ```

### Logical Replication

1. Implement the replication callback behavior:

   ```erlang
   -module(user_replication_handler).
   -behaviour(epg_wal_reader).

   -export([handle_replication_data/2, handle_replication_stop/2]).

   handle_replication_data(_Ref, Changes) ->
       lists:foreach(fun process_change/1, Changes),
       ok.

   handle_replication_stop(_Ref, ReplicationSlot) ->
       logger:info("Replication stopped for slot: ~p", [ReplicationSlot]),
       ok.

   process_change({<<"users">>, insert, UserData}) ->
       logger:info("New user created: ~p", [UserData]);
   process_change({<<"users">>, update, UserData}) ->
       logger:info("User updated: ~p", [UserData]);
   process_change({<<"users">>, delete, UserData}) ->
       logger:info("User deleted: ~p", [UserData]);
   process_change({TableName, Operation, Data}) ->
       logger:info("Change in table ~s: ~p ~p", [TableName, Operation, Data]).
   ```

2. Start logical replication:

   ```erlang
   DbOpts = #{
       host => "localhost",
       port => 5432,
       database => "myapp",
       username => "replication_user",
       password => "replication_pass",
       replication => "database"
   },

   epg_wal_reader:subscription_create(
       user_replication_handler,  % Callback module
       DbOpts,                    % Database connection options
       "myapp_slot",             % Replication slot name
       ["user_changes"]          % Publications to subscribe to
   ).
   ```

## Logical Replication Protocol

`epg_connector` implements PostgreSQL's logical replication protocol with support for:

- **pgoutput plugin** - Native PostgreSQL logical replication output format
- **Real-time streaming** - Continuous WAL (Write-Ahead Log) data streaming
- **Transaction boundaries** - BEGIN/COMMIT message handling
- **Schema information** - Automatic relation metadata decoding
- **Data type conversion** - Automatic conversion of PostgreSQL types to Erlang terms

### Supported Message Types

- `BEGIN` - Transaction start
- `COMMIT` - Transaction commit
- `INSERT` - Row insertion
- `UPDATE` - Row update (with old/new values)
- `DELETE` - Row deletion
- `RELATION` - Table schema information
- `TYPE` - Custom type information
- `TRUNCATE` - Table truncation

## Data Types Support

The connector supports all major PostgreSQL data types:

### Basic Types
- **Integers**: `int2`, `int4`, `int8`
- **Floating Point**: `float4`, `float8`
- **Text**: `text`, `varchar`, `char`, `bpchar`
- **Binary**: `bytea`
- **Boolean**: `bool`

### Date/Time Types
- **Date**: `date`
- **Time**: `time`, `timetz`
- **Timestamp**: `timestamp`, `timestamptz`
- **Interval**: `interval`

### Advanced Types
- **JSON**: `json`, `jsonb`
- **UUID**: `uuid`
- **Arrays**: All array types (e.g., `int4[]`, `text[]`, `jsonb[][][]`)
- **Network**: `inet`, `cidr`, `macaddr`
- **Geometric**: `point`
- **Range Types**: `int4range`, `int8range`, `tsrange`, `tstzrange`

### Type Decoding Limitations

Some PostgreSQL types are not automatically decoded and are returned as text representation (binary strings):

- **Extended types**: `cidr`, `inet`, `macaddr`, `macaddr8`
- **Geometric types**: `point` - returned as text (e.g., `<<"(1,2)">>`)
- **Range types**: `int4range`, `int8range`, `tsrange`, `tstzrange`, `daterange`
- **Advanced types**: `hstore`, `geometry`, `interval`
- **Custom types**: User-defined types and enums - returned as text

These types can be parsed manually in your application logic if needed.

## Examples

### Complete Replication Example

```erlang
-module(order_sync).
-behaviour(epg_wal_reader).

-export([start/0, handle_replication_data/2, handle_replication_stop/2]).

start() ->
    DbOpts = #{
        host => "production-db.example.com",
        port => 5432,
        database => "ecommerce",
        username => "repl_user",
        password => "secure_password",
        replication => "database"
    },

    epg_wal_reader:subscription_create(
        {?MODULE, self()},
        DbOpts,
        "order_replication_slot",
        ["order_events", "inventory_changes"]
    ).

handle_replication_data(_Ref, Changes) ->
    ProcessedChanges = lists:map(fun transform_change/1, Changes),
    send_to_analytics_service(ProcessedChanges),
    update_cache(ProcessedChanges),
    ok.

handle_replication_stop(_Ref, SlotName) ->
    logger:warning("Replication stopped for slot: ~s", [SlotName]),
    % Implement reconnection logic here
    ok.

transform_change({<<"orders">>, insert, OrderData}) ->
    #{
        event_type => order_created,
        table => <<"orders">>,
        order_id => maps:get(<<"id">>, OrderData),
        customer_id => maps:get(<<"customer_id">>, OrderData),
        amount => maps:get(<<"total_amount">>, OrderData),
        timestamp => os:timestamp()
    };
transform_change({<<"orders">>, update, OrderData}) ->
    #{
        event_type => order_updated,
        table => <<"orders">>,
        order_id => maps:get(<<"id">>, OrderData),
        status => maps:get(<<"status">>, OrderData),
        timestamp => os:timestamp()
    };
transform_change({<<"inventory">>, Operation, Data}) ->
    #{
        event_type => inventory_change,
        table => <<"inventory">>,
        operation => Operation,
        data => Data,
        timestamp => os:timestamp()
    };
transform_change({TableName, Operation, Data}) ->
    #{
        event_type => generic_change,
        table => TableName,
        operation => Operation,
        data => Data,
        timestamp => os:timestamp()
    }.
```

### Development Setup

```bash
$ git clone https://github.com/your-repo/epg_connector.git
$ cd epg_connector
$ rebar3 get-deps
$ rebar3 compile
$ rebar3 ct  # Run tests
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---
