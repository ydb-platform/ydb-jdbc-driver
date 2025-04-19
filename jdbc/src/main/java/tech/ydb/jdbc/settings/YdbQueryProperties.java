package tech.ydb.jdbc.settings;

import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import tech.ydb.jdbc.YdbDriver;
import tech.ydb.jdbc.query.QueryType;



/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryProperties {
    private static final Logger LOGGER = Logger.getLogger(YdbDriver.class.getName());

    static final YdbProperty<Boolean> DISABLE_DETECT_SQL_OPERATIONS = YdbProperty.bool("disableDetectSqlOperations",
            "Disable detecting SQL operation based on SQL keywords", false);

    static final YdbProperty<Boolean> DISABLE_PREPARE_DATAQUERY = YdbProperty.bool("disablePrepareDataQuery",
            "Disable executing #prepareDataQuery when creating PreparedStatements", false);

    static final YdbProperty<Boolean> DISABLE_AUTO_PREPARED_BATCHES = YdbProperty.bool("disableAutoPreparedBatches",
            "Disable automatically detect list of tuples or structs in prepared statement", false);

    static final YdbProperty<Boolean> DISABLE_JDBC_PARAMETERS = YdbProperty.bool("disableJdbcParameters",
            "Disable auto detect JDBC standart parameters '?'", false);

    static final YdbProperty<Boolean> FORCE_JDBC_PARAMETERS = YdbProperty.bool("forceJdbcParameters",
            "Enable auto detect JDBC standart parameters '?' for all statements except DDL and DECLARE", false);

    static final YdbProperty<Boolean> REPLACE_JDBC_IN_BY_YQL_LIST = YdbProperty.bool("replaceJdbcInByYqlList",
            "Convert SQL operation IN (?, ?, ... ,?) to YQL operation IN $list", true);

    static final YdbProperty<Boolean> DISABLE_JDBC_PARAMETERS_DECLARE = YdbProperty.bool("disableJdbcParameterDeclare",
            "Disable enforce DECLARE section for JDBC parameters '?'", false);


    @Deprecated
    private static final YdbProperty<QueryType> FORCE_QUERY_MODE = YdbProperty.enums("forceQueryMode", QueryType.class,
            "Force usage one of query modes (DATA_QUERY, SCAN_QUERY, SCHEME_QUERY or EXPLAIN_QUERY) for all statements"
    );
    @Deprecated
    private static final YdbProperty<Boolean> FORCE_SCAN_BULKS = YdbProperty.bool("forceScanAndBulk",
            "Force usage of bulk upserts instead of upserts/inserts and scan query for selects",
            false
    );

    static final YdbProperty<Boolean> REPLACE_INSERT_TO_UPSERT = YdbProperty.bool("replaceInsertByUpsert",
            "Convert all INSERT statements to UPSERT statements", false);
    static final YdbProperty<Boolean> FORCE_BULK_UPSERT = YdbProperty.bool("forceBulkUpsert",
            "Execute all UPSERT statements as BulkUpserts", false);
    static final YdbProperty<Boolean> FORCE_SCAN_SELECT = YdbProperty.bool("forceScanSelect",
            "Execute all SELECT statements as ScanQuery", false);

    private final boolean isDetectQueryType;
    private final boolean isDetectJdbcParameters;
    private final boolean isReplaceJdbcInToYqlList;
    private final boolean isDeclareJdbcParameters;
    private final boolean isForceJdbcParameters;

    private final boolean isPrepareDataQueries;
    private final boolean isDetectBatchQueries;

    private final boolean isReplaceInsertToUpsert;
    private final boolean isForceBulkUpsert;
    private final boolean isForceScanSelect;

    public YdbQueryProperties(YdbConfig config) throws SQLException {
        this(config.getProperties());
    }

    public YdbQueryProperties(Properties props) throws SQLException {
        boolean disableAutoPreparedBatches = DISABLE_AUTO_PREPARED_BATCHES.readValue(props).getValue();
        boolean disablePrepareDataQueries = DISABLE_PREPARE_DATAQUERY.readValue(props).getValue();

        this.isPrepareDataQueries = !disablePrepareDataQueries;
        this.isDetectBatchQueries = !disablePrepareDataQueries && !disableAutoPreparedBatches;

        boolean replaceJdbcInByYqlList = REPLACE_JDBC_IN_BY_YQL_LIST.readValue(props).getValue();
        boolean disableJdbcParametersDeclare = DISABLE_JDBC_PARAMETERS_DECLARE.readValue(props).getValue();
        boolean disableJdbcParametersParse = DISABLE_JDBC_PARAMETERS.readValue(props).getValue();
        boolean disableSqlOperationsDetect = DISABLE_DETECT_SQL_OPERATIONS.readValue(props).getValue();

        this.isDetectQueryType = !disableSqlOperationsDetect;
        this.isForceJdbcParameters = FORCE_JDBC_PARAMETERS.readValue(props).getValue();
        this.isDetectJdbcParameters = isForceJdbcParameters || (isDetectQueryType && !disableJdbcParametersParse);
        this.isDeclareJdbcParameters = isDetectJdbcParameters && !disableJdbcParametersDeclare;
        this.isReplaceJdbcInToYqlList = isDetectJdbcParameters && replaceJdbcInByYqlList;


        YdbValue<QueryType> forcedType = FORCE_QUERY_MODE.readValue(props);
        if (forcedType.hasValue()) {
            LOGGER.warning("Option 'forceQueryMode' is deprecated and will be removed in next versions. "
                    + "Use options 'forceScanSelect' and 'forceBulkUpsert' instead");
        }
        YdbValue<Boolean> forceScanAndBulk = FORCE_SCAN_BULKS.readValue(props);
        if (forceScanAndBulk.hasValue()) {
            LOGGER.warning("Option 'forceScanAndBulk' is deprecated and will be removed in next versions. "
                    + "Use options 'replaceInsertByUpsert', 'forceScanSelect' and 'forceBulkUpsert' instead");
        }

        this.isReplaceInsertToUpsert = REPLACE_INSERT_TO_UPSERT.readValue(props)
                .getValueOrOther(forceScanAndBulk.getValue());
        this.isForceBulkUpsert = FORCE_BULK_UPSERT.readValue(props)
                .getValueOrOther(forceScanAndBulk.getValue() || forcedType.getValue() == QueryType.BULK_QUERY);
        this.isForceScanSelect = FORCE_SCAN_SELECT.readValue(props)
                .getValueOrOther(forceScanAndBulk.getValue() || forcedType.getValue() == QueryType.SCAN_QUERY);
    }

    public boolean isDetectQueryType() {
        return isDetectQueryType;
    }

    public boolean isDetectJdbcParameters() {
        return isDetectJdbcParameters;
    }

    public boolean isDeclareJdbcParameters() {
        return isDeclareJdbcParameters;
    }

    public boolean isReplaceJdbcInByYqlList() {
        return isReplaceJdbcInToYqlList;
    }

    public boolean isPrepareDataQueries() {
        return isPrepareDataQueries;
    }

    public boolean isDetectBatchQueries() {
        return isDetectBatchQueries;
    }

    public boolean isReplaceInsertToUpsert() {
        return isReplaceInsertToUpsert;
    }

    public boolean isForceBulkUpsert() {
        return isForceBulkUpsert;
    }

    public boolean isForceScanSelect() {
        return isForceScanSelect;
    }

    public boolean isForceJdbcParameters() {
        return isForceJdbcParameters;
    }
}
