package tech.ydb.jdbc.context;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

import tech.ydb.core.tracing.Tracer;

/**
 * Resolves {@link tech.ydb.core.tracing.OpenTelemetryTracer} at runtime so the driver does not need
 * OpenTelemetry API on the compile classpath; OTEL API must be present when the URL flag is enabled.
 */
public final class OpenTelemetryTracerLoader {
    private static final String OPENTELEMETRY_TRACER_CLASS = "tech.ydb.core.tracing.OpenTelemetryTracer";

    private OpenTelemetryTracerLoader() {
    }

    public static Tracer loadCreateGlobal() throws SQLException {
        try {
            Class<?> clazz = Class.forName(OPENTELEMETRY_TRACER_CLASS);
            Method createGlobal = clazz.getMethod("createGlobal");
            Object tracer = createGlobal.invoke(null);
            if (tracer instanceof Tracer) {
                return (Tracer) tracer;
            }
            throw new SQLException("OpenTelemetryTracer.createGlobal() did not return a Tracer");
        } catch (ClassNotFoundException e) {
            throw new SQLException("enableOpenTelemetryTracer requires ydb-sdk-core with OpenTelemetryTracer "
                            + "and io.opentelemetry:opentelemetry-api on the classpath", e);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new SQLException("Cannot invoke OpenTelemetryTracer.createGlobal()", e);
        } catch (InvocationTargetException e) {
            throw new SQLException("OpenTelemetryTracer.createGlobal() failed", e);
        }
    }
}
