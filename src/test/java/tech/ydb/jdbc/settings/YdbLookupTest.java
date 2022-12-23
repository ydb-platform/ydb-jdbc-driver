package tech.ydb.jdbc.settings;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class YdbLookupTest {

    @Test
    public void resolveFilePath() throws MalformedURLException {
        Optional<URL> url = YdbLookup.resolvePath("file:/root.file");
        assertEquals(Optional.of(new URL("file:/root.file")), url);
    }

    @Test
    public void resolveClassPath() {
        Optional<URL> url = YdbLookup.resolvePath("classpath:log4j2.xml");
        URL res = ClassLoader.getSystemResource("log4j2.xml");
        assertNotNull(res);
        assertEquals(Optional.of(res), url);
    }

    @Test
    public void resolveFilePathFromHome() throws MalformedURLException {
        Optional<URL> url = YdbLookup.resolvePath("file:~/home.file");
        String home = System.getProperty("user.home");
        assertEquals(Optional.of(new URL("file:" + home + "/home.file")), url);
    }

    @Test
    public void resolveFilePathFromHomePure() throws MalformedURLException {
        Optional<URL> url = YdbLookup.resolvePath("~/home.file");
        String home = System.getProperty("user.home");
        assertEquals(Optional.of(new URL("file:" + home + "/home.file")), url);
    }
}
