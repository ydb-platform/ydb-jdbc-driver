package tech.ydb.jdbc.query;



import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.settings.YdbQueryProperties;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQuery {
    private final ParsedQuery query;
    private final QueryType type;
    private final boolean isAutoDeclare;
    private final List<String> paramNames = new ArrayList<>();

    YdbQuery(ParsedQuery query, QueryType type, boolean isAutoDeclare) {
        this.query = query;
        this.type = type;
        this.isAutoDeclare = isAutoDeclare;
        for (QueryExpression expression : query.getExpressions()) {
            paramNames.addAll(expression.getParamNames());
        }
    }

    public QueryType getType() {
        return type;
    }

    public String getOriginSQL() {
        return query.getOriginSQL();
    }

    public List<QueryExpression> getExpressions() {
        return query.getExpressions();
    }

    public boolean hasFreeParams() {
        return !paramNames.isEmpty();
    }

    public List<String> getFreeParams() {
        return paramNames;
    }

    public String withParams(Params params) throws SQLException {
        if (paramNames.isEmpty()) {
            return query.getPreparedYQL();
        }

        if (params == null) {
            if (!paramNames.isEmpty() && isAutoDeclare) {
                // Comment in place where must be declare section
                return "-- DECLARE " + paramNames.size() + " PARAMETERS\n" + query.getPreparedYQL();
            }
            return query.getPreparedYQL();
        }

        StringBuilder yql = new StringBuilder();
        Map<String, Value<?>> values = params.values();
        for (int idx = 0; idx < paramNames.size(); idx += 1) {
            String prm = paramNames.get(idx);
            if (!values.containsKey(prm)) {
                throw new SQLDataException(YdbConst.MISSING_VALUE_FOR_PARAMETER + prm);
            }

            if (isAutoDeclare) {
                String prmType = values.get(prm).getType().toString();
                yql.append("DECLARE ")
                        .append(prm)
                        .append(" AS ")
                        .append(prmType)
                        .append(";\n");
            }
        }

        yql.append(query.getPreparedYQL());
        return yql.toString();
    }

    public static YdbQuery parseQuery(String queryText, YdbQueryProperties opts) throws SQLException {
        ParsedQuery query = ParsedQuery.parse(queryText, opts);
        QueryType type = opts.getForcedQueryType();
        if (type != null) {
            return new YdbQuery(query, type, opts.isDeclareJdbcParameters());
        }
        for (QueryExpression exp: query.getExpressions()) {
            if (type == null) {
                type = exp.getType();
            } else {
                if (type != exp.getType()) {
                    throw new SQLFeatureNotSupportedException(
                            YdbConst.MULTI_TYPES_IN_ONE_QUERY + type + ", " + exp.getType()
                    );
                }
            }
        }
        if (type == null) {
            type = QueryType.DATA_QUERY;
        }
        return new YdbQuery(query, type, opts.isDeclareJdbcParameters());
    }
}
