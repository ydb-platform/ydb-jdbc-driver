package tech.ydb.jdbc.exception;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;

public class YdbStatusException extends YdbExecutionException {
    private static final long serialVersionUID = -8082086858749679589L;

    private final Status status;

    protected YdbStatusException(String message, String state, Status status) {
        super(message, state, status.getCode().getCode());
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public static YdbStatusException newException(String message, Status status) {
        if (status.getCode().isRetryable(false)) {
            String sqlState = "Retryable[" + status.toString() + "]";
            return new YdbRetryableException(message, sqlState, status);
        }

        if (status.getCode().isRetryable(true)) {
            String sqlState = "ConditionallyRetryable[" + status.toString() + "]";
            return new YdbConditionallyRetryableException(message, sqlState, status);
        }

        String sqlState = "NonRetryable[" + status.toString() + "]";
        return new YdbNonRetryableException(message, sqlState, status);
    }

    public static YdbStatusException newBadRequest(String message) {
        Status status = Status.of(StatusCode.BAD_REQUEST);
        String sqlState = "NonRetryable[" + status.toString() + "]";
        return new YdbNonRetryableException(message, sqlState, status);
    }
}
