$ignored = select 1;
UPSERT INTO #tableName (
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
    c_BankDecimal,

    c_Date32,
    c_Datetime64,
    c_Timestamp64,
    c_Interval64
) VALUES (
    ?, -- key
    ?, -- c_Bool

    ?, -- c_Int8
    ?, -- c_Int16
    ?, -- c_Int32
    ?, -- c_Int64

    ?, -- c_Uint8
    ?, -- c_Uint16
    ?, -- c_Uint32
    ?, -- c_Uint64

    ?, -- c_Float
    ?, -- c_Double

    ?, -- c_Bytes
    ?, -- c_Text
    ?, -- c_Json
    ?, -- c_JsonDocument
    ?, -- c_Yson

    ?, -- c_Uuid

    ?, -- c_Date
    ?, -- c_Datetime
    ?, -- c_Timestamp
    ?, -- c_Interval

    ?, -- c_Decimal
    ?, -- c_BigDecimal
    ?, -- c_BankDecimal

    ?, -- c_Date32
    ?, -- c_Datetime64
    ?, -- c_Timestamp64
    ?  -- c_Interval64
)