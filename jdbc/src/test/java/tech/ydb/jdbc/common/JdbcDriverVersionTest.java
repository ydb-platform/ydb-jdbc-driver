package tech.ydb.jdbc.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 *
 * @author Aleksandr Gorshenin {@literal <alexandr268@ydb.tech>}
 */
public class JdbcDriverVersionTest {

    @Test
    public void basicTest() {
        JdbcDriverVersion version = new JdbcDriverVersion("2.4.0", 2, 4, "2.4.7");

        Assertions.assertEquals("2.4.7", version.getSdkVersion());
        Assertions.assertEquals("2.4.0", version.getDriverVersion());
        Assertions.assertEquals(2, version.getMajor());
        Assertions.assertEquals(4, version.getMinor());
        Assertions.assertEquals("2.4.0(based on SDK 2.4.7)", version.getFullVersion());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "2.4.3",
        "2.4.3-SNAPSHOT",
        "2.4.3-rc1",
    })
    public void sdkCheckTest(String sdk) {
        JdbcDriverVersion version = new JdbcDriverVersion("2.4.0", 2, 4, sdk);

        Assertions.assertTrue(version.isSdkVersion(1));
        Assertions.assertTrue(version.isSdkVersion(2));
        Assertions.assertFalse(version.isSdkVersion(3));

        Assertions.assertTrue(version.isSdkVersion(1, 10));
        Assertions.assertTrue(version.isSdkVersion(2, 3));
        Assertions.assertTrue(version.isSdkVersion(2, 4));
        Assertions.assertFalse(version.isSdkVersion(2, 5));
        Assertions.assertFalse(version.isSdkVersion(3, 0, 0));

        Assertions.assertTrue(version.isSdkVersion(2, 3, 10));
        Assertions.assertTrue(version.isSdkVersion(2, 3, 0));
        Assertions.assertTrue(version.isSdkVersion(2, 4, 3));
        Assertions.assertFalse(version.isSdkVersion(2, 4, 3, 0));
        Assertions.assertFalse(version.isSdkVersion(2, 5));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "unknown",
        "-SNAPSHOT",
        "2.4-rc1.3",
    })
    public void wrongSdkCheckTest(String sdk) {
        JdbcDriverVersion version = new JdbcDriverVersion("2.4.0", 2, 4, sdk);
        Assertions.assertFalse(version.isSdkVersion(2, 4, 3));
    }
}
