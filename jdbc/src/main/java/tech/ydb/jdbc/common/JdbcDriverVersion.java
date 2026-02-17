package tech.ydb.jdbc.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import tech.ydb.core.utils.Version;

public class JdbcDriverVersion {
    private final int major;
    private final int minor;
    private final String version;
    private final String sdk;

    private JdbcDriverVersion(String version, int major, int minor) {
        this.version = version;
        this.major = major;
        this.minor = minor;
        this.sdk = Version.getVersion().orElse("unknown");
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public boolean isSdkVersion(int... vv) {
        try {
            String[] parts = sdk.split("\\.");
            for (int idx = 0; idx < vv.length; idx++) {
                if (idx >= parts.length) {
                    return false;
                }
                if (Integer.parseInt(parts[idx]) < vv[idx]) {
                    return false;
                }
            }
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    public String getDriverVersion() {
        return version;
    }

    public String getFullVersion() {
        return version + "(based on SDK " + sdk + ")";
    }

    public static JdbcDriverVersion getInstance() {
        return Holder.INSTANCE;
    }

    private static class Holder {
        private static final String PROPERTIES_PATH = "/ydb_jdbc.properties";
        private static final JdbcDriverVersion INSTANCE;

        static {
            int major = -1;
            int minor = -1;
            String version = "1.0.development";
            try (InputStream in = Version.class.getResourceAsStream(PROPERTIES_PATH)) {
                Properties prop = new Properties();
                prop.load(in);
                version = prop.getProperty("version");
            } catch (IOException e) { }

            if (version != null && !version.isEmpty()) {
                try {
                    String[] parts = version.split("\\.", 3);
                    if (parts.length > 2) {
                        major = Integer.parseInt(parts[0]);
                        minor = Integer.parseInt(parts[1]);
                    }
                } catch (RuntimeException e) { }
            }

            INSTANCE = new JdbcDriverVersion(version, major, minor);
        }
    }
}
