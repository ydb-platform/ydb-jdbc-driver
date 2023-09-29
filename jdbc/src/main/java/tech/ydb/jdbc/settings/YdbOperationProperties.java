package tech.ydb.jdbc.settings;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import tech.ydb.jdbc.query.QueryType;

public class YdbOperationProperties {
    public static final int MAX_ROWS = 1000; // TODO: how to figure out the max rows of current connection?

    private final Map<YdbOperationProperty<?>, ParsedProperty> params;
    private final Duration joinDuration;
    private final Duration queryTimeout;
    private final Duration scanQueryTimeout;
    private final boolean failOnTruncatedResult;
    private final Duration sessionTimeout;
    private final Duration deadlineTimeout;
    private final boolean autoCommit;
    private final int transactionLevel;
    private final int maxRows;
    private final boolean cacheConnectionsInDriver;

    private final FakeTxMode scanQueryTxMode;
    private final FakeTxMode schemeQueryTxMode;
    private final QueryType forcedQueryType;

    public YdbOperationProperties(Map<YdbOperationProperty<?>, ParsedProperty> params) {
        this.params = Objects.requireNonNull(params);

        this.joinDuration = params.get(YdbOperationProperty.JOIN_DURATION).getParsedValue();
        this.queryTimeout = params.get(YdbOperationProperty.QUERY_TIMEOUT).getParsedValue();
        this.scanQueryTimeout = params.get(YdbOperationProperty.SCAN_QUERY_TIMEOUT).getParsedValue();
        this.failOnTruncatedResult = params.get(YdbOperationProperty.FAIL_ON_TRUNCATED_RESULT).getParsedValue();
        this.sessionTimeout = params.get(YdbOperationProperty.SESSION_TIMEOUT).getParsedValue();
        this.deadlineTimeout = params.get(YdbOperationProperty.DEADLINE_TIMEOUT).getParsedValue();
        this.autoCommit = params.get(YdbOperationProperty.AUTOCOMMIT).getParsedValue();
        this.transactionLevel = params.get(YdbOperationProperty.TRANSACTION_LEVEL).getParsedValue();
        this.maxRows = MAX_ROWS;
        this.cacheConnectionsInDriver = params.get(YdbOperationProperty.CACHE_CONNECTIONS_IN_DRIVER).getParsedValue();

        this.scanQueryTxMode = params.get(YdbOperationProperty.SCAN_QUERY_TX_MODE).getParsedValue();
        this.schemeQueryTxMode = params.get(YdbOperationProperty.SCHEME_QUERY_TX_MODE).getParsedValue();

        ParsedProperty forcedType = params.get(YdbOperationProperty.FORCE_QUERY_MODE);
        this.forcedQueryType = forcedType != null ? forcedType.getParsedValue() : null;
    }

    public Map<YdbOperationProperty<?>, ParsedProperty> getParams() {
        return params;
    }

    public Duration getJoinDuration() {
        return joinDuration;
    }

    public Duration getQueryTimeout() {
        return queryTimeout;
    }

    public Duration getScanQueryTimeout() {
        return scanQueryTimeout;
    }

    public boolean isFailOnTruncatedResult() {
        return failOnTruncatedResult;
    }

    public FakeTxMode getScanQueryTxMode() {
        return scanQueryTxMode;
    }

    public FakeTxMode getSchemeQueryTxMode() {
        return schemeQueryTxMode;
    }

    public QueryType getForcedQueryType() {
        return forcedQueryType;
    }

    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    public Duration getDeadlineTimeout() {
        return deadlineTimeout;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public int getTransactionLevel() {
        return transactionLevel;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public boolean isCacheConnectionsInDriver() {
        return cacheConnectionsInDriver;
    }
}
