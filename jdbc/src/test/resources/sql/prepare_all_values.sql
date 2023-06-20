declare $key as Int32;

declare $c_Bool as Optional<Bool>;
declare $c_Int32 as Optional<Int32>;
declare $c_Int64 as Optional<Int64>;
declare $c_Uint8 as Optional<Uint8>;
declare $c_Uint32 as Optional<Uint32>;
declare $c_Uint64 as Optional<Uint64>;
declare $c_Float as Optional<Float>;
declare $c_Double as Optional<Double>;
declare $c_Bytes as Optional<Bytes>;
declare $c_Text as Optional<Text>;
declare $c_Json as Optional<Json>;
declare $c_JsonDocument as Optional<JsonDocument>;
declare $c_Yson as Optional<Yson>;
declare $c_Date as Optional<Date>;
declare $c_Datetime as Optional<Datetime>;
declare $c_Timestamp as Optional<Timestamp>;
declare $c_Interval as Optional<Interval>;
declare $c_Decimal as Optional<Decimal(22,9)>;

upsert into ${tableName} (
    key,
    c_Bool,
    c_Int32,
    c_Int64,
    c_Uint8,
    c_Uint32,
    c_Uint64,
    c_Float,
    c_Double,
    c_Bytes,
    c_Text,
    c_Json,
    c_JsonDocument,
    c_Yson,
    c_Date,
    c_Datetime,
    c_Timestamp,
    c_Interval,
    c_Decimal
) values (
    $key,
    $c_Bool,
    $c_Int32,
    $c_Int64,
    $c_Uint8,
    $c_Uint32,
    $c_Uint64,
    $c_Float,
    $c_Double,
    $c_Bytes,
    $c_Text,
    $c_Json,
    $c_JsonDocument,
    $c_Yson,
    $c_Date,
    $c_Datetime,
    $c_Timestamp,
    $c_Interval,
    $c_Decimal
)
