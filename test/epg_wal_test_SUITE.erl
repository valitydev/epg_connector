-module(epg_wal_test_SUITE).

-include_lib("stdlib/include/assert.hrl").

%% API
-export([
    init_per_suite/1,
    end_per_suite/1,
    all/0
]).

%% Tests
-export([
    wal_reader_base_test/1,
    wal_connection_lost_test/1,
    wal_persistent_slot_test/1
]).

init_per_suite(Config) ->

    Config.

end_per_suite(_Config) ->
    ok.

all() ->
    [
        wal_reader_base_test,
        wal_connection_lost_test,
        wal_persistent_slot_test
    ].

-spec wal_reader_base_test(_) -> _.
wal_reader_base_test(_C) ->
    _ = mock_subscriber(),
    Publication = "default/default",
    ReplSlot = "test_repl_slot",
    DbOpts = epg_ct_hook:db_opts(),
    Subscriber = {epg_mock_subscriber, self()},
    {ok, Reader} = epg_wal_reader:subscribe(Subscriber, DbOpts, ReplSlot, [Publication]),
    %% INSERT test
    {ok, 1} = epg_pool:query(default_pool, "INSERT INTO t1 (
      bool, int2, int4, int8, float4, float8, char, varchar, text, bytea,
      json, jsonb, date, time, timestamp, timestamptz, timetz, uuid
    ) VALUES (
      true,
      32767,
      2147483647,
      9223372036854775807,
      1.23456789,
      1.2345678901234567,
      'A',
      'example',
      'This is a sample text string',
      $1,
      '{\"name\": \"Alice\", \"age\": 30, \"active\": true}',
      '{\"name\": \"Bob\", \"age\": 25, \"active\": false}',
      '2023-05-15',
      '15:30:45.123456',
      '2023-05-15 15:30:45',
      '2023-05-15 15:30:45.123456+03',
      '15:30:45+03',
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
    )", [<<"POSTGRES">>]),
    {ok, [ReplData1]} = await_replication(),
    Row1 = #{
        <<"bool">> => true,
        <<"bytea">> => <<"POSTGRES">>,
        <<"char">> => 65,
        <<"date">> => {2023,5,15},
        <<"float4">> => 1.2345679,
        <<"float8">> => 1.2345678901234567,
        <<"int2">> => 32767,
        <<"int4">> => 2147483647,
        <<"int8">> => 9223372036854775807,
        <<"json">> => #{
            <<"active">> => true,
            <<"age">> => 30,
            <<"name">> => <<"Alice">>
        },
        <<"jsonb">> => #{
            <<"active">> => false,
            <<"age">> => 25,
            <<"name">> => <<"Bob">>
        },
        <<"jsonb_array">> => null,
        <<"text">> => <<"This is a sample text string">>,
        <<"time">> => {15, 30, 45.123456},
        <<"timestamp">> => {{2023, 5, 15}, {15, 30, 45}},
        <<"timestamptz">> => {{2023, 5, 15}, {12, 30, 45.123456}},
        <<"timetz">> => {{15, 30, 45}, -10800},
        <<"uuid">> => <<"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11">>,
        <<"varchar">> => <<"example">>
    },
    ?assertEqual({<<"t1">>, insert, Row1, #{}}, ReplData1),
    %% UPDATE test
    {ok, 1} = epg_pool:query(default_pool, "UPDATE t1 SET jsonb_array = ARRAY[
        ARRAY[
          '{\"name\": \"Alice\", \"age\": 30}'::jsonb,
          '{\"name\": \"Bob\", \"age\": 25}'::jsonb,
          NULL
        ],
        ARRAY[
          '{\"product\": \"Laptop\", \"price\": 999.99}'::jsonb,
          NULL,
          '{\"product\": \"Phone\", \"accessories\": [\"case\", \"charger\"]}'::jsonb
        ]
      ] WHERE int2 = 32767"
    ),
    {ok, [ReplData2]} = await_replication(),
    ?assertEqual(
        {
            <<"t1">>,
            update,
            #{
                <<"bool">> => true,
                <<"bytea">> => <<"POSTGRES">>,
                <<"char">> => 65,
                <<"date">> => {2023,5,15},
                <<"float4">> => 1.2345679,
                <<"float8">> => 1.2345678901234567,
                <<"int2">> => 32767,
                <<"int4">> => 2147483647,
                <<"int8">> => 9223372036854775807,
                <<"json">> => #{
                    <<"active">> => true,
                    <<"age">> => 30,
                    <<"name">> => <<"Alice">>
                },
                <<"jsonb">> => #{
                    <<"active">> => false,
                    <<"age">> => 25,
                    <<"name">> => <<"Bob">>
                },
                <<"jsonb_array">> => [
                    [
                        #{<<"age">> => 30,<<"name">> => <<"Alice">>},
                        #{<<"age">> => 25,<<"name">> => <<"Bob">>},
                        null
                    ],
                    [
                        #{<<"price">> => 999.99, <<"product">> => <<"Laptop">>},
                        null,
                        #{<<"accessories">> => [<<"case">>,<<"charger">>], <<"product">> => <<"Phone">>}
                    ]
                ],
                <<"text">> => <<"This is a sample text string">>,
                <<"time">> => {15, 30, 45.123456},
                <<"timestamp">> => {{2023, 5, 15}, {15, 30, 45}},
                <<"timestamptz">> => {{2023, 5, 15}, {12, 30, 45.123456}},
                <<"timetz">> => {{15, 30, 45}, -10800},
                <<"uuid">> => <<"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11">>,
                <<"varchar">> => <<"example">>
            },
            Row1
        },
        ReplData2
    ),
    %% DELETE test
    {ok, 1} = epg_pool:query(default_pool, "DELETE FROM t1 where int2 = 32767"),
    {ok, [ReplData3]} = await_replication(),
    {<<"t1">>, delete, #{<<"int2">> := 32767}, #{}} = ReplData3,
    _ = unmock_subscriber(),
    {ok, _, _} = epg_pool:query(default_pool, "TRUNCATE TABLE t1"),
    epg_wal_reader:unsubscribe(Reader),
    ok.

-spec wal_connection_lost_test(_) -> _.
wal_connection_lost_test(_C) ->
    _ = mock_subscriber(),
    Publication = "default/default",
    ReplSlot = "test_repl_slot",
    DbOpts = epg_ct_hook:db_opts(),
    Subscriber = {epg_mock_subscriber, self()},
    {ok, Reader} = epg_wal_reader:subscribe(Subscriber, DbOpts, ReplSlot, [Publication]),
    {ok, ReplConnection} = gen_server:call(Reader, get_connection),
    ok = epgsql:close(ReplConnection),
    {ok, _} = await_stop_replication(),
    ?assertEqual(false, erlang:is_process_alive(Reader)),
    {ok, Reader2} = epg_wal_reader:subscribe(Subscriber, DbOpts, ReplSlot, [Publication]),
    ok = epg_wal_reader:unsubscribe(Reader2),
    timer:sleep(100),
    ?assertEqual(false, erlang:is_process_alive(Reader2)),
    _ = unmock_subscriber(),
    ok.

-spec wal_persistent_slot_test(_) -> _.
wal_persistent_slot_test(_C) ->
    _ = mock_subscriber(),
    Publication = "default/persistent",
    ReplSlot = "test_persistent_repl_slot",
    DbOpts = epg_ct_hook:db_opts(),
    Subscriber = {epg_mock_subscriber, self()},
    ReplOpts = #{slot_type => persistent},
    {ok, Reader1} = epg_wal_reader:subscribe(Subscriber, DbOpts, ReplSlot, [Publication], ReplOpts),
    {ok, _} = epg_pool:transaction(
        default_pool,
        fun(Conn) ->
            {ok, _} = epg_pool:query(Conn, "INSERT INTO t2 (int2, varchar, bytea, jsonb) VALUES
                (1, 'example', $1, '{\"name\": \"Alice\", \"age\": 30, \"active\": true}')", [<<"BYTES1">>]
            ),
            {ok, _} = epg_pool:query(Conn, "UPDATE t2 SET bytea = $1 WHERE int2 = 1", [<<"BYTES2">>])
        end
    ),
    {ok, [
        {<<"t2">>, insert, #{<<"bytea">> := <<"BYTES1">>}, _},
        {<<"t2">>, update, #{<<"bytea">> := <<"BYTES2">>}, _}
    ]} = await_replication(),
    %% stop wal_reader and insert/update data
    ok = epg_wal_reader:unsubscribe(Reader1),
    timer:sleep(100),
    {ok, _} = epg_pool:query(default_pool, "UPDATE t2 SET bytea = $1 WHERE int2 = 1", [<<"BYTES3">>]),
    {ok, _} = epg_pool:query(default_pool, "INSERT INTO t2 (int2, varchar) VALUES (2, 'example2')"),
    {ok, _} = epg_pool:query(default_pool, "INSERT INTO t2 (int2, varchar) VALUES (3, 'example3')"),
    {ok, _} = epg_pool:query(default_pool, "INSERT INTO t2 (int2, varchar) VALUES (4, 'example4')"),
    %% check not replicated
    {error, not_replicated} = await_replication(),
    %% start wal_reader, check data received
    {ok, Reader2} = epg_wal_reader:subscribe(Subscriber, DbOpts, ReplSlot, [Publication], ReplOpts),
    {ok, [{<<"t2">>, update, #{<<"bytea">> := <<"BYTES3">>}, #{<<"bytea">> := <<"BYTES2">>}}]} = await_replication(),
    {ok, [{<<"t2">>, insert, #{<<"int2">> := 2, <<"varchar">> := <<"example2">>}, #{}}]} = await_replication(),
    {ok, [{<<"t2">>, insert, #{<<"int2">> := 3, <<"varchar">> := <<"example3">>}, #{}}]} = await_replication(),
    {ok, [{<<"t2">>, insert, #{<<"int2">> := 4, <<"varchar">> := <<"example4">>}, #{}}]} = await_replication(),
    ok = epg_wal_reader:unsubscribe(Reader2),
    _ = unmock_subscriber(),
    ok.


%% Internal functions

mock_subscriber() ->
    Self = self(),
    meck:new(epg_mock_subscriber, [passthrough]),
    meck:expect(
        epg_mock_subscriber,
        handle_replication_data,
        fun(Ref, ReplData) ->
            V = meck:passthrough([Ref, ReplData]),
            Self ! {repl_data, ReplData},
            V
        end
    ),
    meck:expect(
        epg_mock_subscriber,
        handle_replication_stop,
        fun(Ref, ReplSlot) ->
            V = meck:passthrough([Ref, ReplSlot]),
            Self ! {repl_stop, ReplSlot},
            V
        end
    ).

await_replication() ->
    receive
        {repl_data, ReplData} ->
            {ok, ReplData}
    after 1000 ->
        {error, not_replicated}
    end.

await_stop_replication() ->
    receive
        {repl_stop, ReplSlot} ->
            {ok, ReplSlot}
    after 1000 ->
        {error, not_stopped}
    end.

unmock_subscriber() ->
    meck:unload(epg_mock_subscriber).
