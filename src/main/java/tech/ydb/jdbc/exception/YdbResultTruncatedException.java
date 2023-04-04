package tech.ydb.jdbc.exception;

public class YdbResultTruncatedException extends YdbExecutionException {
    private static final long serialVersionUID = -1887300249465409232L;

    public YdbResultTruncatedException(String reason) {
        super(reason);
    }
}
