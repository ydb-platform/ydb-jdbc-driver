create table #tableName
(
    -- KEY
    key            Int32,
    -- Boolean
    c_Bool         Bool,
    -- Signed integers
    c_Int8         Int8,
    c_Int16        Int16,
    c_Int32        Int32,
    c_Int64        Int64,
    -- Unsigned integers
    c_Uint8        Uint8,
    c_Uint16       Uint16,
    c_Uint32       Uint32,
    c_Uint64       Uint64,

    -- Floats
    c_Float        Float,
    c_Double       Double,

    -- Binary & texts
    c_Bytes        Bytes,
    c_Text         Text,
    c_Json         Json,
    c_JsonDocument JsonDocument,
    c_Yson         Yson,

    -- UUID
    c_Uuid         Uuid,

    -- Dates
    c_Date         Date,
    c_Datetime     Datetime,
    c_Timestamp    Timestamp,
    c_Interval     Interval,

    -- New Dates
    c_Date32       Date32,
    c_Datetime64   Datetime64,
    c_Timestamp64  Timestamp64,
    c_Interval64   Interval64,

    -- Decimal
    c_Decimal      Decimal(22,9),
    c_BigDecimal   Decimal(35, 0),
    c_BankDecimal  Decimal(31, 9),

    -- https://github.com/ydb-platform/ydb/issues/18622
    c_Extra        Int32,
    -- unsupported c_TzDate TzDate,
    -- unsupported c_TzDatetime TzDatetime,
    -- unsupported c_TzTimestamp TzTimestamp,
    -- unsupported c_List List,
    -- unsupported c_Struct Struct,
    -- unsupported c_Tuple Tuple,
    -- unsupported c_Dict Dict,
    -- TODO: support DyNumber?
    primary key (key)
);