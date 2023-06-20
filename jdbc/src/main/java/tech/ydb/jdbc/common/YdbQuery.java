package tech.ydb.jdbc.common;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.settings.YdbOperationProperties;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQuery {
    private final String originSQL;
    private final String originYQL;
    private final QueryType type;
    private final boolean keepInCache;
    private final boolean enforceV1;
    private final List<String> extraParams;

    private YdbQuery(Builder builder) {
        this.originSQL = builder.sql;
        this.type = decodeQueryType(originSQL, builder.props.isDetectSqlOperations());

        this.enforceV1 = builder.props.isEnforceSqlV1();
        this.keepInCache = builder.keepInCache;

        String sql = updateAlternativePrefix(originSQL);
        if (builder.props.isDisableJdbcParametersSupport()) {
            this.originYQL = sql;
            this.extraParams = null;
        } else {
            ArgNameGenerator generator = new ArgNameGenerator(sql);
            this.originYQL = JdbcLexer.jdbc2yql(sql, generator);
            this.extraParams = generator.args;
        }
    }

    public String originSQL() {
        return originSQL;
    }

    public boolean hasFreeParameters() {
        return extraParams != null && !extraParams.isEmpty();
    }

    public String getParameterName(int parameterIndex) {
        if (!hasFreeParameters()) {
            return YdbConst.INDEXED_PARAMETER_PREFIX + parameterIndex;
        }

        if (parameterIndex <= 0 || parameterIndex > extraParams.size()) {
            throw new IllegalArgumentException("Wrong argument index " + parameterIndex);
        }
        return extraParams.get(parameterIndex - 1);
    }

    public String getYqlQuery(Params params) {
        StringBuilder yql = new StringBuilder();

        if (enforceV1) {
            if (!originYQL.contains(YdbConst.PREFIX_SYNTAX_V1)) {
                yql.append(YdbConst.PREFIX_SYNTAX_V1);
                yql.append("\n");
            }
        }

        if (extraParams != null) {
            if (params != null) {
                Map<String, Value<?>> values = params.values();
                for (int idx = 0; idx < extraParams.size(); idx += 1) {
                    String prm = extraParams.get(idx);
                    if (!values.containsKey(prm)) {
                        throw new IllegalArgumentException("Empty JDBC argument " + idx);
                    }

                    String prmType = values.get(prm).getType().toString();
                    yql.append("DECLARE ")
                            .append(prm)
                            .append(" AS ")
                            .append(prmType)
                            .append(";\n");

                }
            } else if (!extraParams.isEmpty()) {
                yql.append("-- DECLARE ").append(extraParams.size()).append(" PARAMETERS").append("\n");
            }
        }

        yql.append(originYQL);
        return yql.toString();
    }

    public boolean keepInCache() {
        return keepInCache;
    }

    public QueryType type() {
        return type;
    }

    private static String updateAlternativePrefix(String sql) {
        String updated = sql;
        for (QueryType type : QueryType.values()) {
            if (updated.contains(type.getAlternativePrefix())) {
                updated = updated.replace(type.getAlternativePrefix(), type.getPrefix()); // Support alternative mode
            }
        }
        return updated;
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

    private static class ArgNameGenerator  implements Supplier<String> {
        private final static String JDBC_ARG_PREFIX = "$jp";

        private final String query;
        private final List<String> args = new ArrayList<>();

        private int nextIdx = 0;

        public ArgNameGenerator(String query) {
            this.query = query;
        }

        @Override
        public String get() {
            while (true) {
                nextIdx += 1;
                String next = JDBC_ARG_PREFIX + nextIdx;
                if (!query.contains(next)) {
                    args.add(next);
                    return next;
                }
            }
        }
    }

    public static Builder from(YdbOperationProperties props, String sql) {
        return new Builder(props, sql);
    }

    public static class Builder {
        private final YdbOperationProperties props;
        private final String sql;
        private boolean keepInCache = true;

        public Builder(YdbOperationProperties props, String sql) {
            this.props = props;
            this.sql = sql;
        }

        public Builder disableCache() {
            this.keepInCache = false;
            return this;
        }

        public YdbQuery build() {
            return new YdbQuery(this);
        }
    }
}
