declare $p1 as Int32;

declare $p2 as Optional<Bool>;

declare $p3 as Optional<Int8>;
declare $p4 as Optional<Int16>;
declare $p5 as Optional<Int32>;
declare $p6 as Optional<Int64>;

declare $p7 as Optional<Uint8>;
declare $p8 as Optional<Uint16>;
declare $p9 as Optional<Uint32>;
declare $p10 as Optional<Uint64>;

declare $p11 as Optional<Float>;
declare $p12 as Optional<Double>;

declare $p13 as Optional<Bytes>;
declare $p14 as Optional<Text>;
declare $p15 as Optional<Json>;
declare $p16 as Optional<JsonDocument>;
declare $p17 as Optional<Yson>;

declare $p18 as Optional<Uuid>;

declare $p19 as Optional<Date>;
declare $p20 as Optional<Datetime>;
declare $p21 as Optional<Timestamp>;
declare $p22 as Optional<Interval>;

declare $p23 as Optional<Decimal(22, 9)>;
declare $p24 as Optional<Decimal(35, 0)>;
declare $p25 as Optional<Decimal(31, 9)>;

upsert into #tableName (
    key,

    c_Bool,

    c_Int8,
    c_Int16,
    c_Int32,
    c_Int64,

    c_Uint8,
    c_Uint16,
    c_Uint32,
    c_Uint64,

    c_Float,
    c_Double,

    c_Bytes,
    c_Text,
    c_Json,
    c_JsonDocument,
    c_Yson,

    c_Uuid,

    c_Date,
    c_Datetime,
    c_Timestamp,
    c_Interval,

    c_Decimal,
    c_BigDecimal,
    c_BankDecimal
) values (
    $p1,
    $p2,
    $p3,
    $p4,
    $p5,
    $p6,
    $p7,
    $p8,
    $p9,
    $p10,
    $p11,
    $p12,
    $p13,
    $p14,
    $p15,
    $p16,
    $p17,
    $p18,
    $p19,
    $p20,
    $p21,
    $p22,
    $p23,
    $p24,
    $p25
)