package tech.ydb.jdbc;

import tech.ydb.jdbc.common.JdbcDriverVersion;

public final class YdbDriverInfo {
    // Driver info
    public static final String DRIVER_NAME = "YDB JDBC Driver";
    public static final int DRIVER_MAJOR_VERSION = JdbcDriverVersion.getInstance().getMajor();
    public static final int DRIVER_MINOR_VERSION = JdbcDriverVersion.getInstance().getMinor();
    public static final String DRIVER_VERSION = JdbcDriverVersion.getInstance().getFullVersion();
    public static final String DRIVER_FULL_NAME = DRIVER_NAME + " " + DRIVER_VERSION;
    public static final int JDBC_MAJOR_VERSION = 4;
    public static final int JDBC_MINOR_VERSION = 2;

    private YdbDriverInfo() { }
}
