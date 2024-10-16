package tech.ydb.jdbc.settings;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Optional;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

import tech.ydb.jdbc.YdbConst;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbLookup {
    private static final String FILE_REF = "file:";
    private static final String CLASSPATH_REF = "classpath:";
    private static final String HOME_REF = "~/";
    private static final String FILE_HOME_REF = FILE_REF + HOME_REF;

    private YdbLookup() { }

    public static String readFileAsString(String name, String value) throws SQLException {
        try (Reader reader = new InputStreamReader(readFile(name, value))) {
            return CharStreams.toString(reader).trim();
        } catch (IOException e) {
            String msg = String.format(YdbConst.INVALID_DRIVER_OPTION_VALUE, value, name, e.getMessage());
            throw new SQLException(msg);
        }
    }

    public static byte[] readFileAsBytes(String name, String value) throws SQLException {
        try (InputStream is = readFile(name, value)) {
            return ByteStreams.toByteArray(is);
        } catch (IOException e) {
            String msg = String.format(YdbConst.INVALID_DRIVER_OPTION_VALUE, value, name, e.getMessage());
            throw new SQLException(msg);
        }
    }

    private static InputStream readFile(String name, String value) throws SQLException, IOException {
        if (value == null || value.trim().isEmpty()) {
            throw new SQLException(YdbConst.MISSING_DRIVER_OPTION + name);
        }

        String path = value.trim();
        if (path.toLowerCase().startsWith(CLASSPATH_REF)) {
            URL resource = ClassLoader.getSystemResource(path.substring(CLASSPATH_REF.length()));
            if (resource == null) {
                String msg = String.format(YdbConst.INVALID_DRIVER_OPTION_VALUE, value, name, "resource not found");
                throw new SQLException(msg);
            }
            return resource.openStream();
        }

        if (path.toLowerCase().startsWith(FILE_REF)) {
            path = path.substring(FILE_REF.length());
        }

        if (path.startsWith(HOME_REF)) {
            String home = System.getProperty("user.home");
            path = path.substring(HOME_REF.length() - 1) + home;
        }

        return new FileInputStream(path);
    }


    public static String stringFileReference(String ref) {
        Optional<URL> urlOpt = resolvePath(ref);
        if (urlOpt.isPresent()) {
            URL url = urlOpt.get();
            try (Reader reader = new InputStreamReader(url.openStream())) {
                return CharStreams.toString(reader).trim();
            } catch (IOException e) {
                throw new RuntimeException("Unable to read resource from " + url, e);
            }
        } else {
            return ref;
        }
    }

    public static byte[] byteFileReference(String ref) {
        Optional<URL> urlOpt = resolvePath(ref);
        if (urlOpt.isPresent()) {
            URL url = urlOpt.get();
            try (InputStream stream = url.openStream()) {
                return ByteStreams.toByteArray(stream);
            } catch (IOException e) {
                throw new RuntimeException("Unable to read resource from " + url, e);
            }
        } else {
            throw new RuntimeException("Must be 'file:' or 'classpath:' reference");
        }
    }

    static Optional<URL> resolvePath(String ref) {
        if (ref.startsWith(HOME_REF) || ref.startsWith(FILE_HOME_REF)) {
            try {
                String home = System.getProperty("user.home");
                String fixedRef = ref.startsWith(HOME_REF)
                        ? ref.substring(HOME_REF.length() - 1)
                        : ref.substring(FILE_HOME_REF.length() - 1);
                return Optional.of(new URL(FILE_REF + home + fixedRef));
            } catch (MalformedURLException e) {
                throw new RuntimeException("Unable to parse ref from home: " + ref, e);
            }
        } else if (ref.startsWith(FILE_REF)) {
            try {
                return Optional.of(new URL(ref));
            } catch (MalformedURLException e) {
                throw new RuntimeException("Unable to parse ref as file: " + ref, e);
            }
        } else if (ref.startsWith(CLASSPATH_REF)) {
            URL systemResource = ClassLoader.getSystemResource(ref.substring(CLASSPATH_REF.length()));
            if (systemResource == null) {
                throw new RuntimeException("Unable to find classpath resource: " + ref);
            }
            return Optional.of(systemResource);
        } else {
            return Optional.empty();
        }
    }

}
