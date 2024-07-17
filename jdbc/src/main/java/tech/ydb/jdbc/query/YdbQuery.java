package tech.ydb.jdbc.query;



import java.sql.SQLException;
import java.util.List;

import tech.ydb.jdbc.settings.YdbQueryProperties;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQuery {
    private final String originQuery;
    private final String preparedYQL;
    private final List<QueryStatement> statements;

    private final QueryType type;
    private final boolean isPlainYQL;

    YdbQuery(String originQuery, String preparedYQL, List<QueryStatement> stats, QueryType type) {
        this.originQuery = originQuery;
        this.preparedYQL = preparedYQL;
        this.statements = stats;
        this.type = type;

        boolean hasJdbcParamters = false;
        for (QueryStatement st: statements) {
            hasJdbcParamters = hasJdbcParamters || !st.getParams().isEmpty();
        }
        this.isPlainYQL = !hasJdbcParamters;
    }

    public QueryType getType() {
        return type;
    }

    public boolean isPlainYQL() {
        return isPlainYQL;
    }

    public String getOriginQuery() {
        return originQuery;
    }

    public String getPreparedYql() {
        return preparedYQL;
    }

    public List<QueryStatement> getStatements() {
        return statements;
    }

//    public String withParams(Params params) throws SQLException {
//        if (isPlainYQL) {
//            return preparedYQL;
//        }
//
//        if (params == null) {
//            if (isAutoDeclare) {
//                int paramCount = statements.stream().mapToInt(st -> st.getParams().size()).sum();
//                // Comment in place where must be declare section
//                return "-- DECLARE " + paramCount + " PARAMETERS\n" + preparedYQL;
//            }
//            return preparedYQL;
//        }
//
//        StringBuilder yql = new StringBuilder();
//        Map<String, Value<?>> values = params.values();
//        for (QueryStatement st: statements) {
//            for (ParamDescription prm: st.getParams()) {
//                if (!values.containsKey(prm.name())) {
//                    throw new SQLDataException(YdbConst.MISSING_VALUE_FOR_PARAMETER + prm);
//                }
//
//                if (isAutoDeclare) {
//                    String prmType = values.get(prm.name()).getType().toString();
//                    yql.append("DECLARE ")
//                            .append(prm.name())
//                            .append(" AS ")
//                            .append(prmType)
//                            .append(";\n");
//                }
//            }
//        }
//
//        yql.append(preparedYQL);
//        return yql.toString();
//    }

    public static YdbQuery parseQuery(String query, YdbQueryProperties opts) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(opts.isDetectQueryType(), opts.isDetectJdbcParameters());
        String preparedYQL = parser.parseSQL(query);

        QueryType type = opts.getForcedQueryType();
        if (type == null) {
            type = parser.detectQueryType();
        }

        return new YdbQuery(query, preparedYQL, parser.getStatements(), type);
    }
}
