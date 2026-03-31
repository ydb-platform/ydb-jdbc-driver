package tech.ydb.jdbc.spi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.core.tracing.NoopTracer;
import tech.ydb.jdbc.context.YdbContext;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.jdbc.settings.YdbConfig;
import tech.ydb.core.tracing.OpenTelemetryTracer;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 * OpenTelemetry on the JDBC gRPC transport: {@code enableOpenTelemetryTracer} selects {@link OpenTelemetryTracer}
 * vs {@link NoopTracer}, and with a test {@link OpenTelemetrySdk} registered globally, spans are exported to an
 * in-memory exporter (same idea as {@code OpenTelemetryQueryTracingIntegrationTest} in ydb-sdk-core).
 */
public class OpenTelemetryTransportTracerTest {

    private static final AttributeKey<String> DB_SYSTEM_NAME = AttributeKey.stringKey("db.system.name");

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcUrlOtel = new JdbcUrlHelper(ydb)
            .withArg("enableOpenTelemetryTracer", "true");

    @BeforeEach
    @AfterEach
    void resetGlobalOpenTelemetry() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void transportTracerDisabledUsesNoopTracer() throws SQLException {
        YdbConfig config = YdbConfig.from(new JdbcUrlHelper(ydb).build(), new Properties());
        try (YdbContext ctx = YdbContext.createContext(config)) {
            Assertions.assertSame(NoopTracer.getInstance(), ctx.getGrpcTransport().getTracer());
        }
    }

    @Test
    void transportTracerEnabledUsesOpenTelemetryTracerImplementation() throws SQLException {
        YdbConfig config = YdbConfig.from(jdbcUrlOtel.build(), new Properties());
        try (YdbContext ctx = YdbContext.createContext(config)) {
            Assertions.assertInstanceOf(OpenTelemetryTracer.class, ctx.getGrpcTransport().getTracer());
        }
    }

    @Test
    void jdbcWithOpenTelemetryExportsYdbSpansToInMemoryExporter() throws SQLException {
        InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();
        try {
            try (Connection conn = DriverManager.getConnection(jdbcUrlOtel.build())) {
                try (ResultSet rs = conn.createStatement().executeQuery("SELECT 1 + 2")) {
                    Assertions.assertTrue(rs.next());
                }
            }

            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            Assertions.assertFalse(spans.isEmpty(), "expected exported spans");

            long ydbSpans = spans.stream()
                    .filter(s -> s.getName().startsWith("ydb."))
                    .count();
            Assertions.assertTrue(ydbSpans >= 1, "expected at least one ydb.* span, got: " + spans);

            long executeQuerySpans = spans.stream()
                    .filter(s -> "ydb.ExecuteQuery".equals(s.getName()))
                    .count();
            Assertions.assertTrue(executeQuerySpans >= 1, "expected ydb.ExecuteQuery span");
            for (SpanData span : spans) {
                if (!"ydb.ExecuteQuery".equals(span.getName())) {
                    continue;
                }
                Assertions.assertEquals(SpanKind.CLIENT, span.getKind());
                Assertions.assertEquals("ydb", span.getAttributes().get(DB_SYSTEM_NAME));
                Assertions.assertEquals(StatusCode.OK, span.getStatus().getStatusCode());
            }
        } finally {
            sdk.close();
        }
    }
}
