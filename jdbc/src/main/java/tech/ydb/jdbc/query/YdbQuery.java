package tech.ydb.jdbc.query;


import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.exception.YdbNonRetryableException;
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
    private final boolean enforceV1;
    private final List<String> extraParams;

    private YdbQuery(YdbOperationProperties props, YdbQueryBuilder builder) {
        this.originSQL = builder.getOriginSQL();
        this.originYQL = builder.buildYQL();
        this.extraParams = builder.getIndexedArgs();
        this.type = builder.getQueryType();

        this.enforceV1 = props.isEnforceSqlV1();
    }

    public String originSQL() {
        return originSQL;
    }

    public boolean hasIndexesParameters() {
        return extraParams != null && !extraParams.isEmpty();
    }

    public List<String> getIndexesParameters() {
        return extraParams;
    }

    public String getYqlQuery(Params params) throws SQLException {
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
                        throw new YdbNonRetryableException(
                                YdbConst.MISSING_VALUE_FOR_PARAMETER + prm,
                                StatusCode.BAD_REQUEST
                        );
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

    public QueryType type() {
        return type;
    }

    public static YdbQuery from(YdbOperationProperties props, String sql) {
        YdbQueryBuilder builder = new YdbQueryBuilder(sql);

        if (!props.isJdbcParametersSupportDisabled()) {
            JdbcLexer.buildQuery(sql, builder, props.isDetectSqlOperations());
        } else {
            builder.append(sql);
        }

        return new YdbQuery(props, builder);
    }
}
