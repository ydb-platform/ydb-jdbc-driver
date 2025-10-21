package tech.ydb.jdbc.impl;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import tech.ydb.jdbc.YdbQueryResult;
import tech.ydb.jdbc.YdbResultSet;
import tech.ydb.jdbc.query.QueryStatement;
import tech.ydb.jdbc.query.YdbQuery;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class YdbQueryResultBase implements YdbQueryResult {
    private static final ResultMeta NO_UPDATED = new ResultMeta(0);
    // TODO: YDB doesn't return the count of affected rows, so we use little hach to return always 1
    private static final ResultMeta HAS_UPDATED = new ResultMeta(1);

    private static class ResultMeta {
        private final int updateCount;
        private final int resultSetIndex;
        private final boolean isGeneratedKeys;

        ResultMeta(int updateCount) {
            this(updateCount, -1, false);
        }

        ResultMeta(int updateCount, int resultSetIndex, boolean isGeneratedKeys) {
            this.updateCount = updateCount;
            this.resultSetIndex = resultSetIndex;
            this.isGeneratedKeys = isGeneratedKeys;
        }
    }

    private final List<ResultMeta> results;
    private int resultIndex;

    public YdbQueryResultBase(int index) { // single query result
        this.results = new ArrayList<>();
        this.resultIndex = 0;
        this.results.add(new ResultMeta(-1, index, false));
    }

    public YdbQueryResultBase(YdbQuery query, int rsCount) {
        this.results = new ArrayList<>();
        this.resultIndex = 0;

        int idx = 0;
        for (QueryStatement exp: query.getStatements()) {
            if (exp.isDDL()) {
                results.add(NO_UPDATED);
                continue;
            }

            if (exp.hasUpdateWithGenerated() && idx < rsCount) {
                results.add(new ResultMeta(1, idx, true));
                idx++;
                continue;
            }

            if (exp.hasUpdateCount()) {
                results.add(HAS_UPDATED);
                continue;
            }

            if (exp.hasResults() && idx < rsCount) {
                results.add(new ResultMeta(-1, idx, false));
                idx++;
            }
        }

        while (idx < rsCount)  {
            results.add(new ResultMeta(-1, idx, false));
            idx++;
        }
    }

    protected abstract YdbResultSet getResultSet(int index) throws SQLException;
    protected abstract void closeResultSet(int index) throws SQLException;

    @Override
    public void close() throws SQLException {
        for (ResultMeta meta: results) {
            if (meta.resultSetIndex >= 0) {
                closeResultSet(meta.resultSetIndex);
            }
        }
    }

    @Override
    public boolean hasResultSets() {
        if (resultIndex >= results.size()) {
            return false;
        }

        ResultMeta exp = results.get(resultIndex);
        return !exp.isGeneratedKeys && exp.resultSetIndex >= 0;
    }

    @Override
    public int getUpdateCount() {
        if (resultIndex >= results.size()) {
            return -1;
        }

        return results.get(resultIndex).updateCount;
    }

    @Override
    public YdbResultSet getCurrentResultSet() throws SQLException  {
        if (resultIndex >= results.size()) {
            return null;
        }

        ResultMeta meta = results.get(resultIndex);
        if (meta.isGeneratedKeys || meta.resultSetIndex < 0) {
            return null;
        }

        return getResultSet(meta.resultSetIndex);
    }

    @Override
    public YdbResultSet getGeneratedKeys() throws SQLException  {
        if (resultIndex >= results.size()) {
            return null;
        }

        ResultMeta meta = results.get(resultIndex);
        if (!meta.isGeneratedKeys || meta.resultSetIndex < 0) {
            return null;
        }

        return getResultSet(meta.resultSetIndex);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (resultIndex >= results.size()) {
            return false;
        }

        if (current == Statement.CLOSE_CURRENT_RESULT && resultIndex < results.size()) {
            closeResultSet(resultIndex);
        }

        if (current == Statement.CLOSE_ALL_RESULTS) {
            for (int idx = 0; idx <= resultIndex && idx < results.size(); idx += 1) {
                closeResultSet(idx);
            }
        }

        resultIndex += 1;
        return hasResultSets();
    }
}
