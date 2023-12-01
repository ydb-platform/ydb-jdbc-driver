package tech.ydb.jdbc.query;



import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQuery {
    private final YdbQueryOptions opts;
    private final String originSQL;
    private final String yqlQuery;
    private final QueryType type;
    private final List<String> indexesArgsNames;
    private final List<YdbExpression> expressions;

    private YdbQuery(YdbQueryOptions opts, YdbQueryBuilder builder) {
        this.opts = opts;
        this.originSQL = builder.getOriginSQL();
        this.yqlQuery = builder.buildYQL();
        this.indexesArgsNames = builder.getIndexedArgs();
        this.type = builder.getQueryType();
        this.expressions = builder.getExpressions();
    }

    public String originSQL() {
        return originSQL;
    }

    public boolean hasIndexesParameters() {
        return indexesArgsNames != null && !indexesArgsNames.isEmpty();
    }

    public List<String> getIndexesParameters() {
        return indexesArgsNames;
    }

    public String getYqlQuery(Params params) throws SQLException {
        StringBuilder yql = new StringBuilder();

        if (indexesArgsNames != null) {
            if (params != null) {
                Map<String, Value<?>> values = params.values();
                for (int idx = 0; idx < indexesArgsNames.size(); idx += 1) {
                    String prm = indexesArgsNames.get(idx);
                    if (!values.containsKey(prm)) {
                        throw new SQLDataException(YdbConst.MISSING_VALUE_FOR_PARAMETER + prm);
                    }

                    if (opts.isDeclareJdbcParameters()) {
                        String prmType = values.get(prm).getType().toString();
                        yql.append("DECLARE ")
                                .append(prm)
                                .append(" AS ")
                                .append(prmType)
                                .append(";\n");
                    }


                }
            } else if (!indexesArgsNames.isEmpty() && opts.isDeclareJdbcParameters()) {
                // Comment in place where must be declare section
                yql.append("-- DECLARE ").append(indexesArgsNames.size()).append(" PARAMETERS").append("\n");
            }
        }

        yql.append(yqlQuery);
        return yql.toString();
    }

    public QueryType type() {
        return type;
    }

    public static YdbQuery from(YdbQueryOptions opts, String sql) throws SQLException {
        YdbQueryBuilder builder = new YdbQueryBuilder(sql, opts.getForcedQueryType());
        JdbcQueryLexer.buildQuery(builder, opts);
        return new YdbQuery(opts, builder);
    }
}
