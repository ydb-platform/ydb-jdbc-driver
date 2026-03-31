package tech.ydb.jdbc.context;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

import tech.ydb.core.tracing.Tracer;

/**
 * Resolves {@code tech.ydb.opentelemetry.OpenTelemetryTracer} at runtime so the driver JAR
 * does not depend on {@code ydb-sdk-opentelemetry} at compile time.
 */
public final class OpenTelemetryTracerLoader {
    private static final String OPENTELEMETRY_TRACER_CLASS = "tech.ydb.opentelemetry.OpenTelemetryTracer";

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
            throw new SQLException("enableOpenTelemetryTracer requires tech.ydb:ydb-sdk-opentelemetry "
                            + "(and OpenTelemetry API) on the classpath", e);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new SQLException("Cannot invoke OpenTelemetryTracer.createGlobal()", e);
        } catch (InvocationTargetException e) {
            throw new SQLException("OpenTelemetryTracer.createGlobal() failed", e);
        }
    }
}
