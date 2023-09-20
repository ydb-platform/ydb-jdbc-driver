package tech.ydb.jdbc.query;



import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.exception.YdbNonRetryableException;
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

    private YdbQuery(YdbQueryOptions opts, YdbQueryBuilder builder) {
        this.opts = opts;
        this.originSQL = builder.getOriginSQL();
        this.yqlQuery = builder.buildYQL();
        this.indexesArgsNames = builder.getIndexedArgs();
        this.type = builder.getQueryType();
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

        if (opts.isEnforceSyntaxV1()) {
            if (!yqlQuery.startsWith(YdbConst.PREFIX_SYNTAX_V1)) {
                yql.append(YdbConst.PREFIX_SYNTAX_V1);
                yql.append("\n");
            }
        }

        if (indexesArgsNames != null) {
            if (params != null) {
                Map<String, Value<?>> values = params.values();
                for (int idx = 0; idx < indexesArgsNames.size(); idx += 1) {
                    String prm = indexesArgsNames.get(idx);
                    if (!values.containsKey(prm)) {
                        throw new YdbNonRetryableException(
                                YdbConst.MISSING_VALUE_FOR_PARAMETER + prm,
                                StatusCode.BAD_REQUEST
                        );
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
        YdbQueryBuilder builder = new YdbQueryBuilder(sql);
        JdbcQueryLexer.buildQuery(builder, opts);
        return new YdbQuery(opts, builder);
    }
}
