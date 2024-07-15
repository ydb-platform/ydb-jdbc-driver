package tech.ydb.jdbc.query;



import java.sql.SQLDataException;
import java.sql.SQLException;
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
    private final String originSQL;
    private final String preparedYQL;
    private final List<QueryStatement> statements;

    private final QueryType type;
    private final boolean isAutoDeclare;
    private final List<String> paramNames = new ArrayList<>();

    YdbQuery(String originSQL, String preparedYQL, List<QueryStatement> stats, QueryType type, boolean isAutoDeclare) {
        this.originSQL = originSQL;
        this.preparedYQL = preparedYQL;
        this.statements = stats;
        this.type = type;
        this.isAutoDeclare = isAutoDeclare;

        for (QueryStatement expression : stats) {
            paramNames.addAll(expression.getParamNames());
        }
    }

    public QueryType getType() {
        return type;
    }

    public String getOriginSQL() {
        return originSQL;
    }

    public List<QueryStatement> getStatements() {
        return statements;
    }

    public boolean hasFreeParams() {
        return !paramNames.isEmpty();
    }

    public List<String> getFreeParams() {
        return paramNames;
    }

    public String withParams(Params params) throws SQLException {
        if (paramNames.isEmpty()) {
            return preparedYQL;
        }

        if (params == null) {
            if (!paramNames.isEmpty() && isAutoDeclare) {
                // Comment in place where must be declare section
                return "-- DECLARE " + paramNames.size() + " PARAMETERS\n" + preparedYQL;
            }
            return preparedYQL;
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

        yql.append(preparedYQL);
        return yql.toString();
    }

    public static YdbQuery parseQuery(String query, YdbQueryProperties opts) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(opts.isDetectQueryType(), opts.isDetectJdbcParameters());
        String preparedYQL = parser.parseSQL(query);

        QueryType type = opts.getForcedQueryType();
        if (type == null) {
            type = parser.detectQueryType();
        }

        return new YdbQuery(query, preparedYQL, parser.getStatements(), type, opts.isDeclareJdbcParameters());
    }
}
