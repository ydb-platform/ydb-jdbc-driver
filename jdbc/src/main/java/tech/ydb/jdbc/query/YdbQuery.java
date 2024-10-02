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
    private final YqlBatcher batch;

    private final QueryType type;
    private final boolean isPlainYQL;

    YdbQuery(String originQuery, String preparedYQL, List<QueryStatement> stats, QueryType type) {
        this(originQuery, preparedYQL, stats, null, type);
    }

    YdbQuery(String originQuery, String preparedYQL, List<QueryStatement> stats, YqlBatcher batch, QueryType type) {
        this.originQuery = originQuery;
        this.preparedYQL = preparedYQL;
        this.statements = stats;
        this.type = type;
        this.batch = batch;

        boolean hasJdbcParamters = false;
        for (QueryStatement st: statements) {
            hasJdbcParamters = hasJdbcParamters || !st.getParams().isEmpty();
        }
        this.isPlainYQL = !hasJdbcParamters;
    }

    public QueryType getType() {
        return type;
    }

    public YqlBatcher getYqlBatcher() {
        return batch;
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

    public static YdbQuery parseQuery(String query, YdbQueryProperties opts) throws SQLException {
        YdbQueryParser parser = new YdbQueryParser(opts.isDetectQueryType(), opts.isDetectJdbcParameters());
        String preparedYQL = parser.parseSQL(query);

        QueryType type = opts.getForcedQueryType();
        if (type == null) {
            type = parser.detectQueryType();
            YqlBatcher batcher = parser.getYqlBatcher();

            if (opts.isForcedScanAndBulks()) {
                if (batcher.isValidBatch()) {
                    if (batcher.getCommand() == YqlBatcher.Cmd.UPSERT) {
                        type = QueryType.BULK_QUERY;
                    }
                    if (batcher.getCommand() == YqlBatcher.Cmd.INSERT) {
                        parser.getYqlBatcher().setForcedUpsert();
                        type = QueryType.BULK_QUERY;
                    }
                }

                if (parser.getStatements().size() == 1 && parser.getStatements().get(0).getCmd() == QueryCmd.SELECT) {
                    type = QueryType.SCAN_QUERY;
                }
            }
        }

        if (parser.getYqlBatcher().isValidBatch()) {
            return new YdbQuery(query, preparedYQL, parser.getStatements(), parser.getYqlBatcher(), type);
        }

        return new YdbQuery(query, preparedYQL, parser.getStatements(), type);
    }
}
