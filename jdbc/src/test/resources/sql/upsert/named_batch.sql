declare $list as List<Struct<
    key:Int32,

    c_Bool:Optional<Bool>,

    c_Int8:Optional<Int8>,
    c_Int16:Optional<Int16>,
    c_Int32:Optional<Int32>,
    c_Int64:Optional<Int64>,

    c_Uint8:Optional<Uint8>,
    c_Uint16:Optional<Uint16>,
    c_Uint32:Optional<Uint32>,
    c_Uint64:Optional<Uint64>,

    c_Float:Optional<Float>,
    c_Double:Optional<Double>,

    c_Bytes:Optional<Bytes>,
    c_Text:Optional<Text>,
    c_Json:Optional<Json>,
    c_JsonDocument:Optional<JsonDocument>,
    c_Yson:Optional<Yson>,

    c_Uuid:Optional<Uuid>,

    c_Date:Optional<Date>,
    c_Datetime:Optional<Datetime>,
    c_Timestamp:Optional<Timestamp>,
    c_Interval:Optional<Interval>,

    c_Decimal:Optional<Decimal(22, 9)>,
    c_BigDecimal:Optional<Decimal(35, 0)>,
    c_BankDecimal:Optional<Decimal(31, 9)>,

    c_Date32:Optional<Date32>,
    c_Datetime64:Optional<Datetime64>,
    c_Timestamp64:Optional<Timestamp64>,
    c_Interval64:Optional<Interval64>
>>;

upsert into #tableName select * from as_table($list)
