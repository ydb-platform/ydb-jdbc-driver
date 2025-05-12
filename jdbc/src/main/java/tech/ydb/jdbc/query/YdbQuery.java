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
    private final YqlBatcher batcher;

    private final QueryType type;
    private final boolean isPlainYQL;

    YdbQuery(String originQuery, String preparedYQL, List<QueryStatement> stats, YqlBatcher batcher, QueryType type) {
        this.originQuery = originQuery;
        this.preparedYQL = preparedYQL;
        this.statements = stats;
        this.type = type;
        this.batcher = batcher;

        boolean hasJdbcParameters = false;
        for (QueryStatement st: statements) {
            hasJdbcParameters = hasJdbcParameters || st.hasJdbcParameters();
        }
        this.isPlainYQL = !hasJdbcParameters;
    }

    public QueryType getType() {
        return type;
    }

    public YqlBatcher getYqlBatcher() {
        return batcher.isValidBatch() ? batcher : null;
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
        YdbQueryParser parser = new YdbQueryParser(query, opts);
        String preparedYQL = parser.parseSQL();

        QueryType type = null;
        YqlBatcher batcher = parser.getYqlBatcher();
        List<QueryStatement> statements = parser.getStatements();

        if (batcher.isValidBatch()) {
            if (batcher.getCommand() == YqlBatcher.Cmd.INSERT && opts.isReplaceInsertToUpsert()) {
                batcher.setForcedUpsert();
            }
            if (batcher.getCommand() == YqlBatcher.Cmd.UPSERT && opts.isForceBulkUpsert()) {
                type = QueryType.BULK_QUERY;
            }
        } else {
            if (opts.isForceScanSelect() && statements.size() == 1 && statements.get(0).getCmd() == QueryCmd.SELECT) {
                if (parser.detectQueryType() == QueryType.DATA_QUERY) { // Only data queries may be converter to SCAN
                    type = QueryType.SCAN_QUERY;
                }
            }
        }

        if (type == null) {
            type = parser.detectQueryType();
        }

        return new YdbQuery(query, preparedYQL, statements, batcher, type);
    }
}
