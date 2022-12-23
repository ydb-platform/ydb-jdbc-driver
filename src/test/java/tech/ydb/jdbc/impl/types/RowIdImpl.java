package tech.ydb.jdbc.impl.types;

import java.sql.RowId;

public class RowIdImpl implements RowId {
    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
