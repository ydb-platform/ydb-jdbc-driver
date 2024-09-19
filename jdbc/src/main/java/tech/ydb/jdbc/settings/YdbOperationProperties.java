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

    static final YdbProperty<Boolean> FORCE_SCAN_BULKS = YdbProperty.bool("forceScanAndBulk",
            "Force usage of bulk upserts instead of upserts/inserts and scan query for selects",
            false
    );

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

    private final YdbValue<Boolean> forceScanBulks;

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

        this.forceScanBulks = FORCE_SCAN_BULKS.readValue(props);
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

    public boolean getForceScanBulks() {
        return forceScanBulks.getValue();
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

    public int getMaxRows() {
        return MAX_ROWS;
    }
}
