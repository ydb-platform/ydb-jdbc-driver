package tech.ydb.jdbc.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import tech.ydb.core.utils.Version;

public class JdbcDriverVersion {
    private static final Logger LOGGER = Logger.getLogger(JdbcDriverVersion.class.getName());

    private final int major;
    private final int minor;
    private final String version;
    private final String sdk;

    private JdbcDriverVersion(String version, int major, int minor, String sdk) {
        this.version = version;
        this.major = major;
        this.minor = minor;
        this.sdk = sdk;
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

                int actual = Integer.parseInt(parts[idx]);
                if (actual > vv[idx]) {
                    return true;
                }
                if (actual < vv[idx]) {
                    return false;
                }
            }
            return true;
        } catch (NumberFormatException ex) {
            LOGGER.log(Level.WARNING, "Cannot parse YDB SDK version " + sdk, ex);
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
            try (InputStream in = JdbcDriverVersion.class.getResourceAsStream(PROPERTIES_PATH)) {
                if (in != null) {
                    Properties prop = new Properties();
                    prop.load(in);
                    version = prop.getProperty("version", version);
                }
            } catch (IOException ex) {
                LOGGER.log(Level.WARNING, "Cannot load YDB JDBC version", ex);
            }

            if (version != null && !version.isEmpty()) {
                try {
                    String[] parts = version.split("\\.", 3);
                    if (parts.length > 2) {
                        major = Integer.parseInt(parts[0]);
                        minor = Integer.parseInt(parts[1]);
                    }
                } catch (RuntimeException ex) {
                    LOGGER.log(Level.WARNING, "Cannot parse YDB JDBC version " + version, ex);
                }
            }

            String sdk = Version.getVersion().orElse("0.0.unknown");
            INSTANCE = new JdbcDriverVersion(version, major, minor, sdk);
        }
    }
}
