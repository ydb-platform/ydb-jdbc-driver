package tech.ydb.jdbc.context;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.tracing.NoopTracer;
import tech.ydb.core.tracing.Span;
import tech.ydb.core.tracing.SpanKind;
import tech.ydb.core.tracing.Tracer;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.test.junit5.YdbHelperExtension;

public class CustomTracerTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcUrl = new JdbcUrlHelper(ydb);

    @Test
    public void noopTracerTest() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl.build())) {
            Assertions.assertSame(NoopTracer.getInstance(), conn.unwrap(GrpcTransport.class).getTracer());
        }
    }

    @Test
    public void customTracerTest() throws SQLException {
        TestTracer custom = new TestTracer();
        Properties props = new Properties();
        props.put("withTracer", custom);
        try (Connection conn = DriverManager.getConnection(jdbcUrl.build(), props)) {
            try (ResultSet rs = conn.createStatement().executeQuery("SELECT 1 + 2")) {
                Assertions.assertTrue(rs.next());
            }
        }
        Assertions.assertTrue(custom.spanCreated > 0);
    }


    private class TestTracer implements Tracer {
        private int spanCreated = 0;

        @Override
        public Span startSpan(String spanName, SpanKind spanKind) {
            spanCreated++;
            return Span.NOOP;
        }

        @Override
        public Span currentSpan() {
            return Span.NOOP;
        }
    }
}
