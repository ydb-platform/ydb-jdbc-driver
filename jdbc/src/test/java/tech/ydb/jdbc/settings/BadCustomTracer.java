package tech.ydb.jdbc.settings;

import tech.ydb.core.tracing.Span;
import tech.ydb.core.tracing.SpanKind;
import tech.ydb.core.tracing.Tracer;

/**
 *
 * @author Aleksandr Gorshenin {@literal <alexandr268@ydb.tech>}
 */
public class BadCustomTracer implements Tracer {

    private final Tracer delegate;

    public BadCustomTracer(Tracer delegate) {
        this.delegate = delegate;
    }

    @Override
    public Span currentSpan() {
        return delegate.currentSpan();
    }

    @Override
    public Span startSpan(String spanName, SpanKind spanKind) {
        return delegate.startSpan(spanName, spanKind);
    }
}
