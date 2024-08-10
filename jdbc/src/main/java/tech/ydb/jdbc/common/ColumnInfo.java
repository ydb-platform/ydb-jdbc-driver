package tech.ydb.jdbc.common;

import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ColumnInfo {
    private final String name;
    private final Type ydbType;
    private final MappingGetters.SqlType sqlType;
    private final MappingGetters.Getters getters;

    private final boolean isOptional;
    private final boolean isTimestamp;
    private final boolean isNumber;
    private final boolean isNull;

    public ColumnInfo(String name, Type type) {
        this.name = name;

        TypeDescription desc = TypeDescription.of(type);
        this.sqlType = desc.sqlType();
        this.getters = desc.getters();
        this.isOptional = desc.isOptional();
        this.ydbType = desc.ydbType();

        this.isTimestamp = type == PrimitiveType.Timestamp;
        this.isNumber = type == PrimitiveType.Int8 || type == PrimitiveType.Uint8
                || type == PrimitiveType.Int16 || type == PrimitiveType.Uint16
                || type == PrimitiveType.Int32 || type == PrimitiveType.Uint32
                || type == PrimitiveType.Int64 || type == PrimitiveType.Uint64;
        this.isNull = type.getKind() == Type.Kind.NULL || type.getKind() == Type.Kind.VOID;
    }

    public String getName() {
        return this.name;
    }

    public Type getYdbType() {
        return this.ydbType;
    }

    public boolean isNull() {
        return isNull;
    }

    public boolean isTimestamp() {
        return isTimestamp;
    }

    public boolean isNumber() {
        return isNumber;
    }

    public boolean isOptional() {
        return isOptional;
    }

    public MappingGetters.SqlType getSqlType() {
        return this.sqlType;
    }

    public MappingGetters.Getters getGetters() {
        return this.getters;
    }

    public static ColumnInfo[] fromResultSetReader(ResultSetReader rsr) {
        ColumnInfo[] columns = new ColumnInfo[rsr.getColumnCount()];
        for (int idx = 0; idx < rsr.getColumnCount(); idx += 1) {
            columns[idx] = new ColumnInfo(rsr.getColumnName(idx), rsr.getColumnType(idx));
        }
        return columns;
    }

}
