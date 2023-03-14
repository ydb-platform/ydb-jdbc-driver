package tech.ydb.jdbc.exception;

public class YdbRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 7195253335276429670L;

    public YdbRuntimeException(String message) {
        super(message);
    }

    public YdbRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
