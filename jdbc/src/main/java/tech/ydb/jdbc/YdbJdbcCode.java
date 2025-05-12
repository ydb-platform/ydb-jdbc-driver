package tech.ydb.jdbc;

/**
 * @author Kirill Kurdyukov
 */
public class YdbJdbcCode {

    private YdbJdbcCode() {
    }

    public static final int DATE_32 = YdbConst.SQL_KIND_PRIMITIVE + 24;

    public static final int DATETIME_64 = YdbConst.SQL_KIND_PRIMITIVE + 25;

    public static final int TIMESTAMP_64 = YdbConst.SQL_KIND_PRIMITIVE + 26;

    public static final int INTERVAL_64 = YdbConst.SQL_KIND_PRIMITIVE + 27;
}
