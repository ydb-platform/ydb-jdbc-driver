package tech.ydb.jdbc.impl.helper;


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import tech.ydb.test.integration.YdbHelperFactory;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class JdbcUrlHelper {
    private final YdbHelperExtension ydb;
    private final String extra;
    private final String authority;
    private final boolean disableToken;

    public JdbcUrlHelper(YdbHelperExtension ydb) {
        this(ydb, "", "", false);
    }

    private JdbcUrlHelper(YdbHelperExtension ydb, String extra, String authority, boolean disableToken) {
        this.ydb = ydb;
        this.extra = extra;
        this.authority = authority;
        this.disableToken = disableToken;
    }

    public JdbcUrlHelper disableToken() {
        return new JdbcUrlHelper(ydb, extra, authority, true);
    }

    public JdbcUrlHelper withArg(String arg, String value) {
        String newExtra = new StringBuilder(extra)
                .append(extra.isEmpty() ? "" : "&")
                .append(encode(arg))
                .append("=")
                .append(encode(value))
                .toString();
        return new JdbcUrlHelper(ydb, newExtra, authority, disableToken);
    }

    public JdbcUrlHelper withAuthority(String username, String password) {
        StringBuilder newAuthority = new StringBuilder(encode(username));
        if (password != null && !password.isEmpty()) {
            newAuthority.append(":").append(encode(password));
        }
        return new JdbcUrlHelper(ydb, extra, newAuthority.append("@").toString(), disableToken);
    }

    public String build() {
        if (!YdbHelperFactory.getInstance().isEnabled()) {
            return "jdbc:ydb:grpc://localhost/local";
        }

        StringBuilder jdbc = new StringBuilder("jdbc:ydb:")
                .append(ydb.useTls() ? "grpcs://" : "grpc://")
                .append(authority)
                .append(ydb.endpoint())
                .append(ydb.database());

        char splitter = '?';
        if (ydb.authToken() != null && !disableToken) {
            jdbc.append(splitter).append("token=").append(ydb.authToken());
            splitter = '&';
        }
        if (!extra.isEmpty()) {
            jdbc.append(splitter).append(extra);
        }

        return jdbc.toString();
    }

    private static String encode(String raw) {
        try {
            return URLEncoder.encode(raw, StandardCharsets.UTF_8.name()).replace("+", "%20");
        } catch (UnsupportedEncodingException ex) {
            return raw;
        }
    }
}
