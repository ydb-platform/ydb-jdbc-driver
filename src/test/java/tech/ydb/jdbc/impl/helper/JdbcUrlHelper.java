package tech.ydb.jdbc.impl.helper;


import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class JdbcUrlHelper {
    private final YdbHelperExtension ydb;
    private final String extra;

    public JdbcUrlHelper(YdbHelperExtension ydb) {
        this.ydb = ydb;
        this.extra = "";
    }

    private JdbcUrlHelper(JdbcUrlHelper other, String extra) {
        this.ydb = other.ydb;
        this.extra = other.extra.isEmpty() ? extra : other.extra +  "&" + extra;
    }

    public JdbcUrlHelper withArg(String arg, String value) {
        return new JdbcUrlHelper(this, arg + "=" + value);
    }

    public String build() {
        StringBuilder jdbc = new StringBuilder("jdbc:ydb:")
                .append(ydb.useTls() ? "grpcs://" : "grpc://")
                .append(ydb.endpoint())
                .append(ydb.database());

        char splitter = '?';
        if (ydb.authToken() != null) {
            jdbc.append(splitter).append("token=").append(ydb.authToken());
            splitter = '&';
        }
        if (!extra.isEmpty()) {
            jdbc.append(splitter).append(extra);
        }

        return jdbc.toString();
    }
}
