package tech.ydb.jdbc.settings;

import tech.ydb.core.tracing.Span;
import tech.ydb.core.tracing.SpanKind;
import tech.ydb.core.tracing.Tracer;

/**
 *
 * @author Aleksandr Gorshenin {@literal <alexandr268@ydb.tech>}
 */
public class CustomTracer implements Tracer {
    @Override
    public Span startSpan(String spanName, SpanKind spanKind) {
        return Span.NOOP;
    }

    @Override
    public Span currentSpan() {
        return Span.NOOP;
    }
}
