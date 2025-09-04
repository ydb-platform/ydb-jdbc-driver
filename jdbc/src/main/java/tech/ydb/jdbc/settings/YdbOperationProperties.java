package tech.ydb.jdbc.settings;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;


public class YdbOperationProperties {
    static final YdbProperty<Duration> JOIN_DURATION = YdbProperty
            .duration("joinDuration", "Default timeout for all YDB operations", "5m");

    static final YdbProperty<Duration> QUERY_TIMEOUT = YdbProperty
            .duration("queryTimeout", "Default timeout for all YDB data queries, scheme and explain operations", "0s");

    static final YdbProperty<Duration> SCAN_QUERY_TIMEOUT = YdbProperty
            .duration("scanQueryTimeout", "Default timeout for all YDB scan queries", "5m");

    static final YdbProperty<Boolean> FAIL_ON_TRUNCATED_RESULT = YdbProperty
            .bool("failOnTruncatedResult", "Throw an exception when received truncated result", false);

    static final YdbProperty<Duration> SESSION_TIMEOUT = YdbProperty
            .duration("sessionTimeout", "Default timeout to create a session", "5s");

    static final YdbProperty<Duration> DEADLINE_TIMEOUT = YdbProperty
            .duration("deadlineTimeout", "Deadline timeout for all operations", "0s");

    static final YdbProperty<Boolean> AUTOCOMMIT = YdbProperty
            .bool("autoCommit", "Auto commit all operations", true);

    static final YdbProperty<Integer> TRANSACTION_LEVEL = YdbProperty
            .integer("transactionLevel", "Default transaction isolation level", Connection.TRANSACTION_SERIALIZABLE);

    static final YdbProperty<FakeTxMode> SCAN_QUERY_TX_MODE = YdbProperty.enums(
            "scanQueryTxMode",
            FakeTxMode.class,
            "Mode of execution scan query inside transaction. Possible values - "
                    + "ERROR(by default), FAKE_TX and SHADOW_COMMIT",
            FakeTxMode.ERROR
    );

    static final YdbProperty<FakeTxMode> SCHEME_QUERY_TX_MODE = YdbProperty.enums("schemeQueryTxMode",
            FakeTxMode.class,
            "Mode of execution scheme query inside transaction. Possible values - "
                    + "ERROR(by default), FAKE_TX and SHADOW_COMMIT",
            FakeTxMode.ERROR
    );

    static final YdbProperty<FakeTxMode> BULK_QUERY_TX_MODE = YdbProperty.enums("bulkUpsertQueryTxMode",
            FakeTxMode.class,
            "Mode of execution bulk upsert query inside transaction. Possible values - "
                    + "ERROR(by default), FAKE_TX and SHADOW_COMMIT",
            FakeTxMode.ERROR
    );

    static final YdbProperty<Boolean> USE_STREAM_RESULT_SETS = YdbProperty.bool("useStreamResultSets",
            "Use stream implementation of ResultSet", false
    );

    static final YdbProperty<Boolean> FORCE_NEW_DATETYPES = YdbProperty.bool("forceSignedDatetimes",
            "Use new data types Date32/Datetime64/Timestamp64 by default", false
    );

    static final YdbProperty<String> TX_VALIDATION_TABLE = YdbProperty.string("withTxValidationTable",
            "Name of working table to store transactions to avoid UNDETERMINED errors");

    static final YdbProperty<String> QUERY_REWRITE_TABLE = YdbProperty.string("withQueryRewriteTable",
            "Name of working table to hot replacemnt of queies");

    static final YdbProperty<Duration> QUERY_REWRITE_TABLE_TTL = YdbProperty.duration("queryRewriteTtl",
            "Name of working table to hot replacemnt of queies", "300s");

    private static final int MAX_ROWS = 1000; // TODO: how to figure out the max rows of current connection?

    private final YdbValue<Duration> joinDuration;
    private final YdbValue<Duration> queryTimeout;
    private final YdbValue<Duration> scanQueryTimeout;
    private final YdbValue<Boolean> failOnTruncatedResult;
    private final YdbValue<Duration> sessionTimeout;
    private final YdbValue<Duration> deadlineTimeout;
    private final YdbValue<Boolean> autoCommit;
    private final YdbValue<Integer> transactionLevel;

    private final YdbValue<FakeTxMode> scanQueryTxMode;
    private final YdbValue<FakeTxMode> schemeQueryTxMode;
    private final YdbValue<FakeTxMode> bulkQueryTxMode;

    private final YdbValue<Boolean> useStreamResultSets;
    private final YdbValue<Boolean> forceNewDatetypes;
    private final YdbValue<String> txValidationTable;
    private final YdbValue<String> queryRewriteTable;
    private final YdbValue<Duration> queryRewriteTTL;

    public YdbOperationProperties(YdbConfig config) throws SQLException {
        Properties props = config.getProperties();

        this.joinDuration = JOIN_DURATION.readValue(props);
        this.queryTimeout = QUERY_TIMEOUT.readValue(props);
        this.scanQueryTimeout = SCAN_QUERY_TIMEOUT.readValue(props);
        this.failOnTruncatedResult = FAIL_ON_TRUNCATED_RESULT.readValue(props);
        this.sessionTimeout = SESSION_TIMEOUT.readValue(props);
        this.deadlineTimeout = DEADLINE_TIMEOUT.readValue(props);
        this.autoCommit = AUTOCOMMIT.readValue(props);
        this.transactionLevel = TRANSACTION_LEVEL.readValue(props);

        this.scanQueryTxMode = SCAN_QUERY_TX_MODE.readValue(props);
        this.schemeQueryTxMode = SCHEME_QUERY_TX_MODE.readValue(props);
        this.bulkQueryTxMode = BULK_QUERY_TX_MODE.readValue(props);

        this.useStreamResultSets = USE_STREAM_RESULT_SETS.readValue(props);
        this.forceNewDatetypes = FORCE_NEW_DATETYPES.readValue(props);
        this.txValidationTable = TX_VALIDATION_TABLE.readValue(props);
        this.queryRewriteTable = QUERY_REWRITE_TABLE.readValue(props);
        this.queryRewriteTTL = QUERY_REWRITE_TABLE_TTL.readValue(props);
    }

    public Duration getJoinDuration() {
        return joinDuration.getValue();
    }

    public Duration getQueryTimeout() {
        return queryTimeout.getValue();
    }

    public Duration getScanQueryTimeout() {
        return scanQueryTimeout.getValue();
    }

    public boolean isFailOnTruncatedResult() {
        return failOnTruncatedResult.getValue();
    }

    public FakeTxMode getScanQueryTxMode() {
        return scanQueryTxMode.getValue();
    }

    public FakeTxMode getSchemeQueryTxMode() {
        return schemeQueryTxMode.getValue();
    }

    public FakeTxMode getBulkQueryTxMode() {
        return bulkQueryTxMode.getValue();
    }

    public Duration getSessionTimeout() {
        return sessionTimeout.getValue();
    }

    public Duration getDeadlineTimeout() {
        return deadlineTimeout.getValue();
    }

    public boolean isAutoCommit() {
        return autoCommit.getValue();
    }

    public int getTransactionLevel() {
        return transactionLevel.getValue();
    }

    public boolean getUseStreamResultSets() {
        return useStreamResultSets.getValue();
    }

    public boolean getForceNewDatetypes() {
        return forceNewDatetypes.getValue();
    }

    public int getMaxRows() {
        return MAX_ROWS;
    }

    public String getTxValidationTable() {
        return txValidationTable.getValue();
    }

    public String getQueryRewriteTable() {
        return queryRewriteTable.getValue();
    }

    public Duration getQueryRewriteTtl() {
        return queryRewriteTTL.getValue();
    }
}
