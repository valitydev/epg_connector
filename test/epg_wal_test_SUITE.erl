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
    wal_connection_lost_test/1
]).

init_per_suite(Config) ->

    Config.

end_per_suite(_Config) ->
    ok.

all() ->
    [
        wal_reader_base_test,
        wal_connection_lost_test
    ].

-spec wal_reader_base_test(_) -> _.
wal_reader_base_test(_C) ->
    _ = mock_subscriber(),
    Publication = "default/default",
    ReplSlot = "test_repl_slot",
    DbOpts = epg_ct_hook:db_opts(),
    Subscriber = epg_mock_subscriber,
    {ok, Reader} = epg_wal_reader:subscription_create(Subscriber, DbOpts, ReplSlot, [Publication]),
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
    ?assertEqual(
        {insert, #{
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
        }},
        ReplData1
    ),
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
        {update,#{
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
        }},
        ReplData2
    ),
    %% DELETE test
    {ok, 1} = epg_pool:query(default_pool, "DELETE FROM t1 where int2 = 32767"),
    {ok, [ReplData3]} = await_replication(),
    {delete, #{<<"int2">> := 32767}} = ReplData3,
    _ = unmock_subscriber(),
    {ok, _, _} = epg_pool:query(default_pool, "TRUNCATE TABLE T1"),
    epg_wal_reader:subscription_delete(Reader),
    ok.

-spec wal_connection_lost_test(_) -> _.
wal_connection_lost_test(_C) ->
    _ = mock_subscriber(),
    Publication = "default/default",
    ReplSlot = "test_repl_slot",
    DbOpts = epg_ct_hook:db_opts(),
    Subscriber = epg_mock_subscriber,
    {ok, Reader} = epg_wal_reader:subscription_create(Subscriber, DbOpts, ReplSlot, [Publication]),
    {ok, ReplConnection} = gen_server:call(Reader, get_connection),
    ok = epgsql:close(ReplConnection),
    {ok, _} = await_stop_replication(),
    ?assertEqual(false, erlang:is_process_alive(Reader)),
    {ok, Reader2} = epg_wal_reader:subscription_create(Subscriber, DbOpts, ReplSlot, [Publication]),
    ok = epg_wal_reader:subscription_delete(Reader2),
    timer:sleep(100),
    ?assertEqual(false, erlang:is_process_alive(Reader2)),
    _ = unmock_subscriber(),
    ok.

%% Internal functions

mock_subscriber() ->
    Self = self(),
    meck:new(epg_mock_subscriber, [passthrough]),
    meck:expect(
        epg_mock_subscriber,
        handle_replication_data,
        fun(ReplData) ->
            V = meck:passthrough([ReplData]),
            Self ! {repl_data, ReplData},
            V
        end
    ),
    meck:expect(
        epg_mock_subscriber,
        handle_replication_stop,
        fun(ReplSlot) ->
            V = meck:passthrough([ReplSlot]),
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
