package tech.ydb.jdbc.exception;

public class YdbResultTruncatedException extends YdbExecutionException {

    public YdbResultTruncatedException(String reason) {
        super(reason);
    }
}
