package tech.ydb.jdbc.common;


import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.settings.YdbOperationProperties;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQuery {
    private final String sql;
    private final String nativeSql;
    private final boolean keepInCache;
    private final QueryType type;

    public YdbQuery(YdbOperationProperties props, String sql, boolean keepInCache) {
        this.sql = sql;
        this.nativeSql = prepareYdbSql(sql, props.isEnforceSqlV1());
        this.type = decodeQueryType(sql, props.isDetectSqlOperations());
        this.keepInCache = keepInCache;
    }

    public String sql() {
        return sql;
    }

    public String nativeSql() {
        return nativeSql;
    }

    public boolean keepInCache() {
        return keepInCache;
    }

    public QueryType type() {
        return type;
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
