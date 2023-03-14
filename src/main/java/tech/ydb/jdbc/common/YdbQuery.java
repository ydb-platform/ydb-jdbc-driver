package tech.ydb.jdbc.common;

import java.time.Duration;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.settings.YdbOperationProperties;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQuery {
    private final String sql;
    private final String nativeSql;
    private final QueryType type;
    private final Duration executionTimeout;

    public YdbQuery(YdbOperationProperties props, String sql) {
        this.sql = sql;
        this.nativeSql = prepareYdbSql(sql, props.isEnforceSqlV1());
        this.type = decodeQueryType(sql, props.isDetectSqlOperations());
        this.executionTimeout = props.getDeadlineTimeout();
    }

    public String sql() {
        return sql;
    }

    public String nativeSql() {
        return nativeSql;
    }

    public QueryType type() {
        return type;
    }

    public Duration executionTimeout() {
        return executionTimeout;
    }

    private static QueryType decodeQueryType(String sql, boolean autoDetect) {
        /*
        Need some logic to figure out - if this is a scheme, data, scan or explain plan query.
        Each mode requires different methods to call.

        TODO: actually implement some logic!
         */
        if (sql != null && autoDetect) {
            for (QueryType type : QueryType.values()) {
                if (sql.contains(type.getPrefix())) {
                    return type;
                }
            }
        }
        return QueryType.DATA_QUERY;
    }

    private static String prepareYdbSql(String sql, boolean enforceV1) {
        for (QueryType type : QueryType.values()) {
            if (sql.contains(type.getAlternativePrefix())) {
                sql = sql.replace(type.getAlternativePrefix(), type.getPrefix()); // Support alternative mode
            }
        }
        if (enforceV1) {
            if (!sql.contains(YdbConst.PREFIX_SYNTAX_V1)) {
                sql = YdbConst.PREFIX_SYNTAX_V1 + "\n" + sql;
            }
        }
        return sql;
    }

    //
}
