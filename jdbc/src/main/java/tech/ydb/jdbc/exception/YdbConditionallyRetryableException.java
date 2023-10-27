package tech.ydb.jdbc.exception;

import tech.ydb.core.Status;

// Treat this as non retryable exception by nature, i.e. need to handle in consciously
public class YdbConditionallyRetryableException extends YdbNonRetryableException {
    private static final long serialVersionUID = -2371144941971339449L;

    YdbConditionallyRetryableException(String message, String sqlState, Status status) {
        super(message, sqlState, status);
    }
}
