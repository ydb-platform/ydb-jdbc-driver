declare $list as List<Struct<
    p1:Int32,

    p2:Optional<Bool>,

    p3:Optional<Int8>,
    p4:Optional<Int16>,
    p5:Optional<Int32>,
    p6:Optional<Int64>,

    p7:Optional<Uint8>,
    p8:Optional<Uint16>,
    p9:Optional<Uint32>,
    p10:Optional<Uint64>,

    p11:Optional<Float>,
    p12:Optional<Double>,

    p13:Optional<Bytes>,
    p14:Optional<Text>,
    p15:Optional<Json>,
    p16:Optional<JsonDocument>,
    p17:Optional<Yson>,

    p18:Optional<Uuid>,

    p19:Optional<Date>,
    p20:Optional<Datetime>,
    p21:Optional<Timestamp>,
    p22:Optional<Interval>,

    p23:Optional<Decimal(22, 9)>,
    p24:Optional<Decimal(35, 0)>,
    p25:Optional<Decimal(31, 9)>,

    p26:Optional<Date32>,
    p27:Optional<Datetime64>,
    p28:Optional<Timestamp64>,
    p29:Optional<Interval64>
>>;

upsert into #tableName select
    p1 as key,

    p2 as c_Bool,

    p3 as c_Int8,
    p4 as c_Int16,
    p5 as c_Int32,
    p6 as c_Int64,

    p7 as c_Uint8,
    p8 as c_Uint16,
    p9 as c_Uint32,
    p10 as c_Uint64,

    p11 as c_Float,
    p12 as c_Double,

    p13 as c_Bytes,
    p14 as c_Text,
    p15 as c_Json,
    p16 as c_JsonDocument,
    p17 as c_Yson,

    p18 as c_Uuid,

    p19 as c_Date,
    p20 as c_Datetime,
    p21 as c_Timestamp,
    p22 as c_Interval,

    p23 as c_Decimal,
    p24 as c_BigDecimal,
    p25 as c_BankDecimal,

    p26 as c_Date32,
    p27 as c_Datetime64,
    p28 as c_Timestamp64,
    p29 as c_Interval64
from as_table($list);
