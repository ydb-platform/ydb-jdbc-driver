package tech.ydb.jdbc.query;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import tech.ydb.jdbc.settings.ParsedProperty;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.jdbc.settings.YdbOperationProperty;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryOptions {
    private final boolean isDetectQueryType;
    private final boolean isDetectJdbcParameters;
    private final boolean isDeclareJdbcParameters;

    private final boolean isPrepareDataQueries;
    private final boolean isDetectBatchQueries;

    private final QueryType forcedType;

    @VisibleForTesting
    YdbQueryOptions(
            boolean detectQueryType,
            boolean detectJbdcParams,
            boolean declareJdbcParams,
            boolean prepareDataQuery,
            boolean detectBatchQuery,
            QueryType forcedType
    ) {
        this.isDetectQueryType = detectQueryType;
        this.isDetectJdbcParameters = detectJbdcParams;
        this.isDeclareJdbcParameters = declareJdbcParams;

        this.isPrepareDataQueries = prepareDataQuery;
        this.isDetectBatchQueries = detectBatchQuery;

        this.forcedType = forcedType;
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

    public static YdbQueryOptions createFrom(YdbOperationProperties props) {
        boolean declareJdbcParams = true;
        boolean detectJbdcParams = true;
        boolean detectBatchQuery = true;
        boolean prepareDataQuery = true;
        boolean detectQueryType = true;

        // forced properies
        Map<YdbOperationProperty<?>, ParsedProperty> params = props.getParams();

        if (params.containsKey(YdbOperationProperty.DISABLE_AUTO_PREPARED_BATCHES)) {
            boolean v = params.get(YdbOperationProperty.DISABLE_AUTO_PREPARED_BATCHES).getParsedValue();
            detectBatchQuery = !v;
        }

        if (params.containsKey(YdbOperationProperty.DISABLE_PREPARE_DATAQUERY)) {
            boolean v = params.get(YdbOperationProperty.DISABLE_PREPARE_DATAQUERY).getParsedValue();
            prepareDataQuery = !v;
            detectBatchQuery = detectBatchQuery && prepareDataQuery;
        }

        if (params.containsKey(YdbOperationProperty.DISABLE_JDBC_PARAMETERS_DECLARE)) {
            boolean v = params.get(YdbOperationProperty.DISABLE_JDBC_PARAMETERS_DECLARE).getParsedValue();
            declareJdbcParams = !v;
        }

        if (params.containsKey(YdbOperationProperty.DISABLE_JDBC_PARAMETERS)) {
            boolean v = params.get(YdbOperationProperty.DISABLE_JDBC_PARAMETERS).getParsedValue();
            detectJbdcParams = !v;
            declareJdbcParams = declareJdbcParams && detectJbdcParams;
        }

        if (params.containsKey(YdbOperationProperty.DISABLE_DETECT_SQL_OPERATIONS)) {
            boolean v = params.get(YdbOperationProperty.DISABLE_DETECT_SQL_OPERATIONS).getParsedValue();
            detectQueryType = !v;
            detectJbdcParams = detectJbdcParams && detectQueryType;
            declareJdbcParams = declareJdbcParams && detectJbdcParams;
        }

        QueryType forcedQueryType = props.getForcedQueryType();

        return new YdbQueryOptions(
                detectQueryType,
                detectJbdcParams,
                declareJdbcParams,
                prepareDataQuery,
                detectBatchQuery,
                forcedQueryType
        );
    }
}
