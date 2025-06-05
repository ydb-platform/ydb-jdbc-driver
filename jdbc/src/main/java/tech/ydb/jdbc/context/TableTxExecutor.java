package tech.ydb.jdbc.context;

import java.sql.SQLException;

import com.google.common.hash.Hashing;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.exception.ExceptionFactory;
import tech.ydb.jdbc.exception.YdbConditionallyRetryableException;
import tech.ydb.jdbc.impl.YdbQueryResult;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.query.QueryStream;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;

/**
 *
 * @author mzinal
 */
public class TableTxExecutor extends QueryServiceExecutor {
    private static final String CREATE_SQL = ""
            + "CREATE TABLE IF NOT EXISTS `%s` ("
            + "   hash Text NOT NULL,"
            + "   tx_id Text NOT NULL,"
            + "   committed_at Timestamp,"
            + "   PRIMARY KEY (hash, tx_id)"
            + ") WITH ("
            + "   TTL=Interval('PT60M') ON committed_at,"
            + "   AUTO_PARTITIONING_BY_LOAD=ENABLED,"
            + "   AUTO_PARTITIONING_BY_SIZE=ENABLED,"
            + "   AUTO_PARTITIONING_PARTITION_SIZE_MB=100"
            + ");";

    private static final String COMMIT_SQL = ""
            + "DECLARE $hash AS Text; "
            + "DECLARE $tx AS Text; "
            + "UPSERT INTO `%s` (hash, tx_id, committed_at) VALUES ($hash, $tx, CurrentUtcTimestamp());";

    private static final String VALIDATE_SQL = ""
            + "DECLARE $hash AS Text; "
            + "DECLARE $tx AS Text; "
            + "SELECT hash, tx_id FROM `%s` WHERE hash=$hash AND tx_id=$tx;";

    private final String commitQuery;
    private final String validateQuery;
    private final String txTablePath;
    private boolean isWriteTx;

    public TableTxExecutor(YdbContext ctx, String tablePath) throws SQLException {
        super(ctx);
        this.txTablePath = tablePath;
        this.commitQuery = String.format(COMMIT_SQL, tablePath);
        this.validateQuery = String.format(VALIDATE_SQL, tablePath);
        this.isWriteTx = false;
    }

    @Override
    public void rollback(YdbContext ctx, YdbValidator validator) throws SQLException {
        isWriteTx = false;
        super.rollback(ctx, validator);
    }

    @Override
    public YdbQueryResult executeDataQuery(YdbStatement statement, YdbQuery query, String preparedYql, Params params,
            long timeout, boolean keepInCache) throws SQLException {
        YdbQueryResult result = super.executeDataQuery(statement, query, preparedYql, params, timeout, keepInCache);
        isWriteTx = isInsideTransaction() && (isWriteTx || query.isWriting());

        return result;
    }

    @Override
    protected void commitImpl(YdbContext ctx, YdbValidator validator, QueryTransaction tx) throws SQLException {
        boolean storeTx = isWriteTx;
        isWriteTx = false;
        if (!storeTx) {
            super.commitImpl(ctx, validator, tx);
            return;
        }

        String hash = Hashing.sha256().hashBytes(tx.getId().getBytes()).toString();
        Params params = Params.of(
                "$hash", PrimitiveValue.newText(hash),
                "$tx", PrimitiveValue.newText(tx.getId())
        );

        YdbTracer tracer = ctx.getTracer();
        ExecuteQuerySettings settings = ctx.withRequestTimeout(ExecuteQuerySettings.newBuilder()).build();
        try {
            QueryStream query = tx.createQuery(commitQuery, true, params, settings);
            validator.clearWarnings();
            validator.call("CommitAndStore TxId: " + tx.getId(), tracer, () -> {
                tracer.trace("--> commit-and-store-tx " + hash);
                tracer.query(commitQuery);
                return query.execute();
            });
        } catch (YdbConditionallyRetryableException ex) {

            try (Session session = createNewTableSession(validator)) {
                tracer.trace("--> validate tx");
                tracer.query(validateQuery);
                Result<DataQueryResult> res = session.executeDataQuery(validateQuery, TxControl.snapshotRo(), params)
                        .join();
                if (res.isSuccess()) {
                    DataQueryResult dqr = res.getValue();
                    if (dqr.getResultSetCount() == 1) {
                        if (dqr.getResultSet(0).getRowCount() == 1) {
                            // Transaction was committed successfully
                            return;
                        } else {
                            // Transaction wann't commit
                            Status status = Status.of(StatusCode.ABORTED).withCause(ex);
                            throw ExceptionFactory.createException("Transaction wasn't committed",
                                    new UnexpectedResultException("Transaction not found in " + txTablePath, status)
                            );
                        }
                    }
                }
            }

            throw ex;
        }
    }

    public static TableDescription validate(YdbContext ctx, String tablePath) throws SQLException {
        // validate table name
        Result<TableDescription> res = ctx.getRetryCtx().supplyResult(s -> s.describeTable(tablePath)).join();
        if (res.isSuccess()) {
            return res.getValue();
        }

        if (res.getStatus().getCode() != StatusCode.SCHEME_ERROR) {
            throw ExceptionFactory.createException(
                    "Cannot initialize TableTxExecutor with tx table " + tablePath,
                    new UnexpectedResultException("Cannot describe", res.getStatus()));
        }

        // Try to create a table
        String query = String.format(CREATE_SQL, tablePath);
        Status status = ctx.getRetryCtx().supplyStatus(session -> session.executeSchemeQuery(query)).join();
        if (!status.isSuccess()) {
            throw ExceptionFactory.createException(
                    "Cannot initialize TableTxExecutor with tx table " + tablePath,
                    new UnexpectedResultException("Cannot create table", status));
        }

        Result<TableDescription> res2 = ctx.getRetryCtx().supplyResult(s -> s.describeTable(tablePath)).join();
        if (!res2.isSuccess()) {
            throw ExceptionFactory.createException(
                    "Cannot initialize TableTxExecutor with tx table " + tablePath,
                    new UnexpectedResultException("Cannot describe after creating", res2.getStatus()));
        }

        return res2.getValue();
    }
}
