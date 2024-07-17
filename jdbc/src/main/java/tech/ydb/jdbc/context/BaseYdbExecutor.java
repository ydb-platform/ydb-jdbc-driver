package tech.ydb.jdbc.context;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import tech.ydb.core.Result;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.query.QueryType;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.impl.ProtoValueReaders;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.settings.ExecuteSchemeQuerySettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public abstract class BaseYdbExecutor implements YdbExecutor {
    private final Duration sessionTimeout;
    private final TableClient tableClient;

    public BaseYdbExecutor(YdbContext ctx) {
        this.sessionTimeout = ctx.getOperationProperties().getSessionTimeout();
        this.tableClient = ctx.getTableClient();
    }

    protected Session createNewTableSession(YdbValidator validator) throws SQLException {
        try {
            Result<Session> session = tableClient.createSession(sessionTimeout).join();
            validator.addStatusIssues(session.getStatus());
            return session.getValue();
        } catch (UnexpectedResultException ex) {
            throw ExceptionFactory.createException("Cannot create session with " + ex.getStatus(), ex);
        }
    }

    @Override
    public void executeSchemeQuery(YdbContext ctx, YdbValidator validator, String yql) throws SQLException {
        // Scheme query does not affect transactions or result sets
        ExecuteSchemeQuerySettings settings = ctx.withDefaultTimeout(new ExecuteSchemeQuerySettings());
        try (Session session = createNewTableSession(validator)) {
            validator.execute(QueryType.SCHEME_QUERY + " >>\n" + yql, () -> session.executeSchemeQuery(yql, settings));
        }
    }

    @Override
    public ResultSetReader executeScanQuery(YdbContext ctx, YdbValidator validator, String yql, Params params)
            throws SQLException {
        ensureOpened();

        Collection<ResultSetReader> resultSets = new LinkedBlockingQueue<>();
        Duration scanQueryTimeout = ctx.getOperationProperties().getScanQueryTimeout();
        ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder()
                .withRequestTimeout(scanQueryTimeout)
                .build();
        try (Session session = createNewTableSession(validator)) {
            validator.execute(QueryType.SCAN_QUERY + " >>\n" + yql,
                    () -> session.executeScanQuery(yql, params, settings).start(resultSets::add));
        }

        return ProtoValueReaders.forResultSets(resultSets);
    }


}
