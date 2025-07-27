-module(epg_wal_reader).

-behaviour(gen_server).

-include("epg_replication.hrl").
-include("epg_oids.hrl").
-include_lib("epgsql/include/epgsql.hrl").

-export([subscription_create/4]).
-export([subscription_delete/1]).
-export([handle_x_log_data/4]).

-export([parse_array/1]).

-export([start_link/4]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-callback handle_replication_data([{inset | delete | update, map()}]) -> ok.
-callback handle_replication_stop(ReplSlot :: string()) -> ok.

-type wal_state() :: #{
    subscriber := module(),
    repl_slot := string(),
    db_opts := epgsql:connect_opts_map(),
    publications := [string()],
    tables := map(),
    rows := list(),
    connection => pid()
}.

subscription_create(Subscriber, DbOpts, ReplSlot, ListPublications) ->
    ChildSpec = #{
        id => ReplSlot,
        start => {?MODULE, start_link, [Subscriber, DbOpts, ReplSlot, ListPublications]},
        restart => temporary
    },
    supervisor:start_child(epg_connector_sup, ChildSpec).

subscription_delete(Ref) ->
    gen_server:cast(Ref, stop).

start_link(Subscriber, DbOpts, ReplSlot, ListPublications) ->
    gen_server:start_link({local, reg_name(ReplSlot)}, ?MODULE, [Subscriber, DbOpts, ReplSlot, ListPublications], []).

reg_name(ReplSlot) ->
    erlang:list_to_atom(lists:concat(["repl_slot_", ReplSlot])).

handle_replication_msg(Handler, Msg) ->
    gen_server:cast(Handler, Msg).

%% gen_server callbacks
-spec init(_) -> {ok, wal_state()}.
init([Subscriber, DbOpts, ReplSlot, ListPublications]) ->
    erlang:process_flag(trap_exit, true),
    State0 = #{
        subscriber => Subscriber,
        repl_slot => ReplSlot,
        db_opts => DbOpts,
        publications => ListPublications,
        tables => #{},
        rows => []
    },
    {ok, State} = connect(State0),
    ok = create_replication_slot(State),
    ok = start_replication(State),
    {ok, State}.

handle_call(get_connection, _From, State) ->
    {reply, {ok, maps:get(connection, State, undefined)}, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({pgoutput_msg, _StartLSN, EndLSN, #commit_msg{}}, #{rows := []} = State) ->
    {noreply, State#{last_processed_lsn => EndLSN}};
handle_cast({pgoutput_msg, _StartLSN, EndLSN, #commit_msg{}}, #{rows := Rows} = State) ->
    #{subscriber := Subscriber} = State,
    ok = Subscriber:handle_replication_data(Rows),
    {noreply, State#{last_processed_lsn => EndLSN, rows => []}};
handle_cast({pgoutput_msg, _StartLSN, EndLSN, #relation_msg{} = RelationInfo}, State) ->
    Relidentifier = RelationInfo#relation_msg.id,
    #{tables := Tables} = State,
    NewState = State#{
        tables => Tables#{Relidentifier => RelationInfo},
        last_processed_lsn => EndLSN
    },
    {noreply, NewState};
handle_cast({pgoutput_msg, _StartLSN, EndLSN, #row_msg{} = RowMsg}, State) ->
    #row_msg{relation_id = Relidentifier, msg_type = MsgType, columns = ColumnsValues} = RowMsg,
    #{tables := Tables, rows := Rows} = State,
    RelationInfo = maps:get(Relidentifier, Tables),
    ColumnsInfo = RelationInfo#relation_msg.columns,
    Row = aggregate_row(ColumnsInfo, ColumnsValues),
    NewState = State#{
        last_processed_lsn => EndLSN,
        rows => [{MsgType, Row} | Rows]
    },
    {noreply, NewState};
handle_cast(stop, #{connection := Connection}) ->
    _ = epgsql:close(Connection),
    exit(normal);
handle_cast(stop, _State) ->
    exit(normal);
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Info}, #{connection := Pid, repl_slot := ReplSlot, subscriber := Subscriber}) ->
    ok = Subscriber:handle_replication_stop(ReplSlot),
    exit(normal);
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% replication handler callbacks

handle_x_log_data(_StartLSN, EndLSN, <<>>, CallbackState) ->
    {ok, EndLSN, EndLSN, CallbackState};
handle_x_log_data(StartLSN, EndLSN, Data, #{handler := Handler} = CallbackState) ->
    {ok, Message} = epg_pgoutput_decoder:decode(Data),
    handle_replication_msg(Handler, {pgoutput_msg, StartLSN, EndLSN, Message}),
    {ok, EndLSN, EndLSN, CallbackState}.

%% Internal functions
start_replication(#{connection := Connection} = State) ->
    #{
        repl_slot := ReplSlot,
        publications := ListPublications
    } = State,
    Pubs = lists:foldl(
        fun
            (PubName, "") ->
                "\"" ++ PubName ++ "\"";
            (PubName, Acc) ->
                Acc ++ ", \"" ++ PubName ++ "\""
        end,
        "",
        ListPublications
    ),
    CallbackModule = ?MODULE,
    CallbackInitState = #{handler => self()},
    StartLSN = "0/0",
    %% raw text
    PluginOpts = "proto_version '1', publication_names '" ++ Pubs ++ "'",
    ReplicationOpts = [{temporary, true}],
    epgsql:start_replication(
        Connection,
        ReplSlot,
        CallbackModule,
        CallbackInitState,
        StartLSN,
        PluginOpts,
        ReplicationOpts
    ).

create_replication_slot(#{repl_slot := ReplSlot, connection := Connection}) ->
    case epgsql:squery(Connection, ["CREATE_REPLICATION_SLOT ", ReplSlot, " TEMPORARY LOGICAL pgoutput"]) of
        {ok, _, _} ->
            ok;
        {error, #error{codename = duplicate_object}} ->
            ok
    end.

connect(#{db_opts := #{database := DB} = DbOpts} = State) ->
    try epgsql:connect(DbOpts#{replication => "database"}) of
        {ok, Connection} ->
            logger:info("db replication connection established. database: ~p", [DB]),
            {ok, State#{connection => Connection}};
        {error, _Reason} ->
            %% TODO log it
            {error, not_connected}
    catch
        _:_ ->
            %% TODO log it
            {error, not_connected}
    end.

aggregate_row(ColumnsInfo, ColumnsValues) ->
    {Row, []} = lists:foldl(
        fun
            (
                #column_value{kind = null},
                {Row, [#relation_column{name = Name} | Rest]}
            ) ->
                {Row#{Name => null}, Rest};
            (
                #column_value{kind = unchanged},
                {Row, [#relation_column{} | Rest]}
            ) ->
                %% ignore column
                {Row, Rest};
            (
                #column_value{kind = text, value = Value},
                {Row, [#relation_column{name = Name, data_type_id = DataTypeId} | Rest]}
            ) ->
                {Row#{Name => decode(DataTypeId, Value)}, Rest}
        end,
        {#{}, ColumnsInfo},
        ColumnsValues
    ),
    Row.

decode(_, null) ->
    null;
decode(?BOOL, <<"t">>) ->
    true;
decode(?BOOL, <<"f">>) ->
    false;
decode(INT, Value) when INT =:= ?INT2; INT =:= ?INT4; INT =:= ?INT8 ->
    binary_to_integer(Value);
decode(FLOAT, Value) when FLOAT =:= ?FLOAT4; FLOAT =:= ?FLOAT8 ->
    binary_to_float(Value);
decode(?CHAR, <<ASCII>>) ->
    ASCII;
%% TODO maybe bug
decode(?BPCHAR, <<ASCII>>) ->
    ASCII;
decode(?VARCHAR, Value) ->
    Value;
decode(?TEXT, Value) ->
    Value;
decode(?BYTEA, <<"\\x", Hex/binary>>) ->
    binary:list_to_bin([binary_to_integer(<<X, Y>>, 16) || <<X, Y>> <= Hex]);
decode(JSON, Value) when JSON =:= ?JSON; JSON =:= ?JSONB ->
    jsx:decode(Value, [return_maps]);
decode(?UUID, Value) ->
    Value;
decode(?DATE, <<Year:4/binary, "-", Month:2/binary, "-", Day:2/binary>>) ->
    {
        binary_to_integer(Year),
        binary_to_integer(Month),
        binary_to_integer(Day)
    };
decode(?TIME, Value) ->
    [HourBin, MinuteBin, SecondBin] = binary:split(Value, <<":">>, [global]),
    {binary_to_integer(HourBin), binary_to_integer(MinuteBin), binary_to_num(SecondBin)};
decode(?TIMETZ, Value) ->
    [HourBin, MinuteBin, SecondWihtTzBin] = binary:split(Value, <<":">>, [global]),
    {Second, TZ} = parse_seconds_with_tz(SecondWihtTzBin),
    {{binary_to_integer(HourBin), binary_to_integer(MinuteBin), Second}, TZ};
decode(?TIMESTAMP, Value) ->
    [DateBin, TimeBin] = binary:split(Value, <<" ">>),
    Date = decode(?DATE, DateBin),
    Time = decode(?TIME, TimeBin),
    {Date, Time};
decode(?TIMESTAMPTZ, Value) ->
    [DateBin, TimeBin] = binary:split(Value, <<" ">>),
    Date = decode(?DATE, DateBin),
    {{Hour, Minute, Second}, TZ} = decode(?TIMETZ, TimeBin),
    IntegerPart = trunc(Second),
    FractionalPart = Second - IntegerPart,
    DateTime = {Date, {Hour, Minute, IntegerPart}},
    GregorianSeconds = calendar:datetime_to_gregorian_seconds(DateTime),
    NewGregorianSeconds = GregorianSeconds + TZ,
    {NewDate, {NewHour, NewMinute, NewSecond}} = calendar:gregorian_seconds_to_datetime(NewGregorianSeconds),
    {NewDate, {NewHour, NewMinute, NewSecond + FractionalPart}};
decode(ArrayType, Value) when
    ArrayType =:= ?BOOL_ARRAY;
    ArrayType =:= ?INT2_ARRAY;
    ArrayType =:= ?INT4_ARRAY;
    ArrayType =:= ?INT8_ARRAY;
    ArrayType =:= ?FLOAT4_ARRAY;
    ArrayType =:= ?FLOAT8_ARRAY;
    ArrayType =:= ?CHAR_ARRAY;
    ArrayType =:= ?BPCHAR_ARRAY;
    ArrayType =:= ?VARCHAR_ARRAY;
    ArrayType =:= ?TEXT_ARRAY;
    ArrayType =:= ?BYTEA_ARRAY;
    ArrayType =:= ?JSON_ARRAY;
    ArrayType =:= ?JSONB_ARRAY;
    ArrayType =:= ?DATE_ARRAY;
    ArrayType =:= ?TIME_ARRAY;
    ArrayType =:= ?TIMETZ_ARRAY;
    ArrayType =:= ?TIMESTAMP_ARRAY;
    ArrayType =:= ?TIMESTAMPTZ_ARRAY;
    ArrayType =:= ?UUID_ARRAY
->
    Array = parse_array(Value),
    decode_array(?ARRAY_ITEM(ArrayType), Array, []);
decode(_Type, Value) ->
    %% not implemented, returning as is
    Value.
%

decode_array(_Type, [], Result) ->
    lists:reverse(Result);
decode_array(Type, [Item | Tail], Acc) ->
    decode_array(Type, Tail, [decode_array_item(Type, Item) | Acc]).

decode_array_item(Type, Item) when is_list(Item) ->
    decode_array(Type, Item, []);
decode_array_item(_Type, <<"NULL">>) ->
    null;
decode_array_item(Type, Value) ->
    decode(Type, Value).

binary_to_num(Bin) ->
    try
        binary_to_float(Bin)
    catch
        _:_ ->
            binary_to_integer(Bin)
    end.

parse_seconds_with_tz(SecondWihtTzBin) ->
    maybe
        [SecondBin, HourPlus] ?= binary:split(SecondWihtTzBin, <<"+">>),
        {binary_to_num(SecondBin), -1 * binary_to_num(HourPlus) * 3600}
    else
        _ ->
            [SecondBin2, HourMinus] = binary:split(SecondWihtTzBin, <<"-">>),
            {binary_to_num(SecondBin2), binary_to_num(HourMinus) * 3600}
    end.

%% @doc Парсит текстовое представление многомерного массива PostgreSQL
-spec parse_array(binary()) -> list().
parse_array(<<"{", Rest/binary>>) ->
    {Result, _} = parse_array_content(Rest, []),
    Result;
parse_array(Binary) ->
    error({invalid_array_format, Binary}).

%% Парсит содержимое массива
parse_array_content(<<"}", Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
parse_array_content(<<"}">>, Acc) ->
    {lists:reverse(Acc), <<>>};
parse_array_content(Binary, Acc) ->
    {Element, Rest} = parse_element(Binary),
    case Rest of
        <<",", Rest2/binary>> ->
            parse_array_content(skip_whitespace(Rest2), [Element | Acc]);
        _ ->
            parse_array_content(Rest, [Element | Acc])
    end.

%% Парсит один элемент массива
parse_element(<<"{", _/binary>> = Binary) ->
    % Вложенный массив
    parse_nested_array(Binary);
parse_element(<<"NULL", Rest/binary>>) ->
    {null, skip_whitespace_and_comma(Rest)};
parse_element(<<"\"", Rest/binary>>) ->
    % Строка в кавычках
    parse_quoted_string(Rest, <<>>);
parse_element(Binary) ->
    % Простое значение без кавычек (не должно встречаться в данном контексте)
    parse_unquoted_value(Binary, <<>>).

%% Парсит вложенный массив
parse_nested_array(<<"{", Rest/binary>>) ->
    parse_nested_array_content(Rest, [], 1).

parse_nested_array_content(Binary, Acc, 0) ->
    {lists:reverse(Acc), Binary};
parse_nested_array_content(<<"{", Rest/binary>>, Acc, Level) ->
    parse_nested_array_content(Rest, [<<"{">> | Acc], Level + 1);
parse_nested_array_content(<<"}", Rest/binary>>, Acc, Level) when Level > 1 ->
    parse_nested_array_content(Rest, [<<"}">> | Acc], Level - 1);
parse_nested_array_content(<<"}", Rest/binary>>, Acc, 1) ->
    % Собираем содержимое вложенного массива и парсим рекурсивно
    Content = iolist_to_binary(lists:reverse(Acc)),
    ArrayBinary = <<"{", Content/binary, "}">>,
    ParsedArray = parse_array(ArrayBinary),
    {ParsedArray, skip_whitespace_and_comma(Rest)};
parse_nested_array_content(<<C, Rest/binary>>, Acc, Level) ->
    parse_nested_array_content(Rest, [<<C>> | Acc], Level).

%% Парсит строку в кавычках с учетом экранирования
parse_quoted_string(<<"\"", Rest/binary>>, Acc) ->
    {Acc, skip_whitespace_and_comma(Rest)};
parse_quoted_string(<<"\\\"", Rest/binary>>, Acc) ->
    parse_quoted_string(Rest, <<Acc/binary, "\"">>);
parse_quoted_string(<<"\\\\", Rest/binary>>, Acc) ->
    parse_quoted_string(Rest, <<Acc/binary, "\\">>);
parse_quoted_string(<<C, Rest/binary>>, Acc) ->
    parse_quoted_string(Rest, <<Acc/binary, C>>);
parse_quoted_string(<<>>, Acc) ->
    {Acc, <<>>}.

%% Парсит значение без кавычек
parse_unquoted_value(<<",", Rest/binary>>, Acc) ->
    {Acc, Rest};
parse_unquoted_value(<<"}", Rest/binary>>, Acc) ->
    {Acc, <<"}", Rest/binary>>};
parse_unquoted_value(<<C, Rest/binary>>, Acc) when C =/= $\s, C =/= $\t, C =/= $\n ->
    parse_unquoted_value(Rest, <<Acc/binary, C>>);
parse_unquoted_value(<<C, _/binary>> = Rest, Acc) when C =:= $\s; C =:= $\t; C =:= $\n ->
    {Acc, skip_whitespace_and_comma(Rest)};
parse_unquoted_value(<<>>, Acc) ->
    {Acc, <<>>}.

%% Пропускает пробелы и запятые
skip_whitespace_and_comma(Binary) ->
    Binary2 = skip_whitespace(Binary),
    case Binary2 of
        <<",", Rest/binary>> -> skip_whitespace(Rest);
        _ -> Binary2
    end.

%% Пропускает пробелы
skip_whitespace(<<C, Rest/binary>>) when C =:= $\s; C =:= $\t; C =:= $\n; C =:= $\r ->
    skip_whitespace(Rest);
skip_whitespace(Binary) ->
    Binary.
