package tech.ydb.jdbc.settings;

import java.sql.SQLException;
import java.util.Properties;

import tech.ydb.jdbc.query.QueryType;



/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryProperties {
    static final YdbProperty<Boolean> DISABLE_DETECT_SQL_OPERATIONS = YdbProperty.bool("disableDetectSqlOperations",
            "Disable detecting SQL operation based on SQL keywords", false);

    static final YdbProperty<Boolean> DISABLE_PREPARE_DATAQUERY = YdbProperty.bool("disablePrepareDataQuery",
            "Disable executing #prepareDataQuery when creating PreparedStatements", false);

    static final YdbProperty<Boolean> DISABLE_AUTO_PREPARED_BATCHES = YdbProperty.bool("disableAutoPreparedBatches",
            "Disable automatically detect list of tuples or structs in prepared statement", false);

    static final YdbProperty<Boolean> DISABLE_JDBC_PARAMETERS = YdbProperty.bool("disableJdbcParameters",
            "Disable auto detect JDBC standart parameters '?'", false);

    static final YdbProperty<Boolean> DISABLE_JDBC_PARAMETERS_DECLARE = YdbProperty.bool("disableJdbcParameterDeclare",
            "Disable enforce DECLARE section for JDBC parameters '?'", false);

    static final YdbProperty<QueryType> FORCE_QUERY_MODE = YdbProperty.enums("forceQueryMode", QueryType.class,
            "Force usage one of query modes (DATA_QUERY, SCAN_QUERY, SCHEME_QUERY or EXPLAIN_QUERYn) for all statements"
    );

    private final boolean isDetectQueryType;
    private final boolean isDetectJdbcParameters;
    private final boolean isDeclareJdbcParameters;

    private final boolean isPrepareDataQueries;
    private final boolean isDetectBatchQueries;

    private final QueryType forcedType;

    public YdbQueryProperties(YdbConfig config) throws SQLException {
        Properties props = config.getProperties();

        boolean disableAutoPreparedBatches = DISABLE_AUTO_PREPARED_BATCHES.readValue(props).getValue();
        boolean disablePrepareDataQueries = DISABLE_PREPARE_DATAQUERY.readValue(props).getValue();

        this.isPrepareDataQueries = !disablePrepareDataQueries;
        this.isDetectBatchQueries = !disablePrepareDataQueries && !disableAutoPreparedBatches;

        boolean disableJdbcParametersDeclare = DISABLE_JDBC_PARAMETERS_DECLARE.readValue(props).getValue();
        boolean disableJdbcParametersParse = DISABLE_JDBC_PARAMETERS.readValue(props).getValue();
        boolean disableSqlOperationsDetect = DISABLE_DETECT_SQL_OPERATIONS.readValue(props).getValue();

        this.isDetectQueryType = !disableSqlOperationsDetect;
        this.isDetectJdbcParameters = !disableSqlOperationsDetect && !disableJdbcParametersParse;
        this.isDeclareJdbcParameters = !disableSqlOperationsDetect && !disableJdbcParametersParse && !disableJdbcParametersDeclare;

        this.forcedType = FORCE_QUERY_MODE.readValue(props).getValue();
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

    public boolean iPrepareDataQueries() {
        return isPrepareDataQueries;
    }

    public boolean isDetectBatchQueries() {
        return isDetectBatchQueries;
    }

    public QueryType getForcedQueryType() {
        return forcedType;
    }
}
