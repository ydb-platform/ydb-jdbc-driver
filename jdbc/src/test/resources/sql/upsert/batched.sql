declare $list as List<Struct<
    key:Int32,

    c_Bool:Bool?,

    c_Int8:Int8?,
    c_Int16:Int16?,
    c_Int32:Int32?,
    c_Int64:Int64?,

    c_Uint8:Uint8?,
    c_Uint16:Uint16?,
    c_Uint32:Uint32?,
    c_Uint64:Uint64?,

    c_Float:Float?,
    c_Double:Double?,

    c_Bytes:Bytes?,
    c_Text:Text?,
    c_Json:Json?,
    c_JsonDocument:JsonDocument?,
    c_Yson:Yson?,

    c_Date:Date?,
    c_Datetime:Datetime?,
    c_Timestamp:Timestamp?,
    c_Interval:Interval?,

    c_Decimal:Decimal(22,9)?
>>;

upsert into #tableName select * from as_table($list)
