%% PostgreSQL OID definitions
%% Generated from type information

%% Basic types
-define(BOOL, 16).
-define(BOOL_ARRAY, 1000).

-define(INT2, 21).
-define(INT2_ARRAY, 1005).

-define(INT4, 23).
-define(INT4_ARRAY, 1007).

-define(INT8, 20).
-define(INT8_ARRAY, 1016).

-define(FLOAT4, 700).
-define(FLOAT4_ARRAY, 1021).

-define(FLOAT8, 701).
-define(FLOAT8_ARRAY, 1022).

-define(CHAR, 18).
-define(CHAR_ARRAY, 1002).

-define(BPCHAR, 1042).
-define(BPCHAR_ARRAY, 1014).

-define(VARCHAR, 1043).
-define(VARCHAR_ARRAY, 1015).

-define(TEXT, 25).
-define(TEXT_ARRAY, 1009).

-define(BYTEA, 17).
-define(BYTEA_ARRAY, 1001).

-define(JSON, 114).
-define(JSON_ARRAY, 199).

-define(JSONB, 3802).
-define(JSONB_ARRAY, 3807).

-define(ARRAY_ITEM(ARRAY_TYPE),
    case ARRAY_TYPE of
        ?BOOL_ARRAY -> ?BOOL;
        ?INT2_ARRAY -> ?INT2;
        ?INT4_ARRAY -> ?INT4;
        ?INT8_ARRAY -> ?INT8;
        ?FLOAT4_ARRAY -> ?FLOAT4;
        ?FLOAT8_ARRAY -> ?FLOAT8;
        ?CHAR_ARRAY -> ?CHAR;
        ?BPCHAR_ARRAY -> ?BPCHAR;
        ?VARCHAR_ARRAY -> ?VARCHAR;
        ?TEXT_ARRAY -> ?TEXT;
        ?BYTEA_ARRAY -> ?BYTEA;
        ?JSON_ARRAY -> ?JSON;
        ?JSONB_ARRAY -> ?JSONB;
        ?DATE_ARRAY -> ?DATE;
        ?TIME_ARRAY -> ?TIME;
        ?TIMETZ_ARRAY -> ?TIMETZ;
        ?TIMESTAMP_ARRAY -> ?TIMESTAMP;
        ?TIMESTAMPTZ_ARRAY -> ?TIMESTAMPTZ;
        ?UUID_ARRAY -> ?UUID
    end
).

%% Date/Time types
-define(DATE, 1082).
-define(DATE_ARRAY, 1182).

-define(TIME, 1083).
-define(TIME_ARRAY, 1183).

-define(TIMESTAMP, 1114).
-define(TIMESTAMP_ARRAY, 1115).

-define(TIMESTAMPTZ, 1184).
-define(TIMESTAMPTZ_ARRAY, 1185).

-define(TIMETZ, 1266).
-define(TIMETZ_ARRAY, 1270).

-define(UUID, 2950).
-define(UUID_ARRAY, 2951).

%% Extended types
-define(CIDR, 650).
-define(CIDR_ARRAY, 651).

-define(DATERANGE, 3912).
-define(DATERANGE_ARRAY, 3913).

-define(GEOMETRY, 17063).
-define(GEOMETRY_ARRAY, 17071).

-define(HSTORE, 16935).
-define(HSTORE_ARRAY, 16940).

-define(INET, 869).
-define(INET_ARRAY, 1041).

-define(INT4RANGE, 3904).
-define(INT4RANGE_ARRAY, 3905).

-define(INT8RANGE, 3926).
-define(INT8RANGE_ARRAY, 3927).

-define(INTERVAL, 1186).
-define(INTERVAL_ARRAY, 1187).

-define(MACADDR, 829).
-define(MACADDR_ARRAY, 1040).

-define(MACADDR8, 774).
-define(MACADDR8_ARRAY, 775).

-define(POINT, 600).
-define(POINT_ARRAY, 1017).

-define(TSRANGE, 3908).
-define(TSRANGE_ARRAY, 3909).

-define(TSTZRANGE, 3910).
-define(TSTZRANGE_ARRAY, 3911).
