package tech.ydb.jdbc.exception;

import tech.ydb.core.StatusCode;

// Treat this as non retryable exception by nature, i.e. need to handle in consciously
public class YdbConditionallyRetryableException extends YdbNonRetryableException {
    private static final long serialVersionUID = 1135970796364528563L;

    public YdbConditionallyRetryableException(Object response, StatusCode statusCode) {
        super(response, statusCode);
    }
}
