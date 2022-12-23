package tech.ydb.jdbc.settings;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbLookup {
    private static final String FILE_REF = "file:";
    private static final String CLASSPATH_REF = "classpath:";
    private static final String HOME_REF = "~";
    private static final String FILE_HOME_REF = FILE_REF + HOME_REF;

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
                        ? ref.substring(HOME_REF.length())
                        : ref.substring(FILE_HOME_REF.length());
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
