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

    public String getFullVersion() {
        return version + "(based on SDK " + sdk + ")";
    }

    public static JdbcDriverVersion getInstance() {
        return Holder.instance;
    }

    private static class Holder {
        private final static String PROPERTIES_PATH = "/ydb_jdbc.properties";
        private final static JdbcDriverVersion instance;

        static {
            int major = -1;
            int minor = -1;
            String version = "1.0.development";
            try {
                Properties prop = new Properties();
                InputStream in = Version.class.getResourceAsStream(PROPERTIES_PATH);
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

            instance = new JdbcDriverVersion(version, major, minor);
        }
    }
}
