package tech.ydb.jdbc.impl.helper;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TestConsts {
    private TestConsts() { }

    public static String allTypesTable(String name) {
        return "create table " + name + "\n"
                + "(\n"
                + "    key            Int32,\n"
                + "    c_Bool         Bool,\n"
                + "    -- unsupported, c_Int8        Int8,\n"
                + "    -- unsupported, c_Int16        Int16,\n"
                + "    c_Int32        Int32,\n"
                + "    c_Int64        Int64,\n"
                + "    c_Uint8        Uint8,\n"
                + "    -- unsupported, c_Uint16        Uint16,\n"
                + "    c_Uint32       Uint32,\n"
                + "    c_Uint64       Uint64,\n"
                + "    c_Float        Float,\n"
                + "    c_Double       Double,\n"
                + "    c_Bytes        Bytes,\n"
                + "    c_Text         Text,\n"
                + "    c_Json         Json,\n"
                + "    c_JsonDocument JsonDocument,\n"
                + "    c_Yson         Yson,\n"
                + "    c_Date         Date,\n"
                + "    c_Datetime     Datetime,\n"
                + "    c_Timestamp    Timestamp,\n"
                + "    c_Interval     Interval,\n"
                + "    c_Decimal      Decimal(22,9),\n"
                + "    -- unsupported c_Uuid Uuid,\n"
                + "    -- unsupported c_TzDate TzDate,\n"
                + "    -- unsupported c_TzDatetime TzDatetime,\n"
                + "    -- unsupported c_TzTimestamp TzTimestamp,\n"
                + "    -- unsupported c_List List,\n"
                + "    -- unsupported c_Struct Struct,\n"
                + "    -- unsupported c_Tuple Tuple,\n"
                + "    -- unsupported c_Dict Dict,\n"
                + "    primary key (key)\n"
                + ");\n";
    }
}
