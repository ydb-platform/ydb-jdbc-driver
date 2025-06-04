package tech.ydb.jdbc.context;

import java.sql.SQLException;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbStatement;
import tech.ydb.jdbc.YdbTracer;
import tech.ydb.jdbc.impl.YdbQueryResult;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.query.QueryTransaction;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;

/**
 *
 * @author mzinal
 */
public class QueryServiceExecutorExt extends QueryServiceExecutor {

    private final String processUndeterminedTable;
    private boolean writing;

    public QueryServiceExecutorExt(YdbContext ctx, int transactionLevel, boolean autoCommit) throws SQLException {
        super(ctx, transactionLevel, autoCommit);
        this.processUndeterminedTable = ctx.getOperationProperties().getProcessUndeterminedTable();
        this.writing = false;
    }

    private Status upsertAndCommit(QueryTransaction localTx) {
        String sql = "DECLARE $trans_hash AS Int32; DECLARE $trans_id AS Text; "
                + "UPSERT INTO `" + processUndeterminedTable
                + "` (trans_hash, trans_id, trans_tv) "
                + "VALUES ($trans_hash, $trans_id, CurrentUtcTimestamp());";
        Params params = Params.of(
                "$trans_id", PrimitiveValue.newText(localTx.getId()),
                "$trans_hash", PrimitiveValue.newInt32(localTx.getId().hashCode())
        );
        return localTx.createQueryWithCommit(sql, params)
                .execute()
                .join()
                .getStatus();
    }

    private boolean checkTransaction(YdbContext ctx, String transId,
            YdbValidator validator, YdbTracer tracer) throws SQLException {
        String sql = "DECLARE $trans_hash AS Int32; DECLARE $trans_id AS Text; "
                + "SELECT trans_id, trans_tv FROM `" + processUndeterminedTable
                + "` WHERE trans_hash=$trans_hash AND trans_id=$trans_id;";
        Params params = Params.of(
                "$trans_id", PrimitiveValue.newText(transId),
                "$trans_hash", PrimitiveValue.newInt32(transId.hashCode())
        );
        Result<DataQueryResult> result = ctx.getRetryCtx().supplyResult(
                session -> session.executeDataQuery(sql, TxControl.onlineRo(), params))
                .join();
        if (!result.getStatus().isSuccess()) {
            // Failed to obtain the transaction status, have to return the error
            validator.validate("CommitVal TxId: " + transId, tracer, result.getStatus());
        }
        DataQueryResult dqr = result.getValue();
        if (dqr.getResultSetCount() == 1) {
            if (dqr.getResultSet(0).getRowCount() == 1) {
                return true;
            }
        }
        return false;
    }

    private void commitWithCheck(YdbContext ctx, YdbValidator validator) throws SQLException {
        ensureOpened();

        QueryTransaction localTx = tx.get();
        if (localTx == null || !localTx.isActive()) {
            return;
        }

        YdbTracer tracer = ctx.getTracer();
        tracer.trace("--> commitExt");
        tracer.query(null);

        try {
            validator.clearWarnings();
            Status status = upsertAndCommit(localTx);
            if (StatusCode.UNDETERMINED.equals(status.getCode())) {
                if (checkTransaction(ctx, localTx.getId(), validator, tracer)) {
                    status = Status.SUCCESS;
                } else {
                    status = Status.of(StatusCode.ABORTED, status.getCause(), status.getIssues());
                }
            }
            validator.validate("CommitExt TxId: " + localTx.getId(), tracer, status);
        } finally {
            if (tx.compareAndSet(localTx, null)) {
                localTx.getSession().close();
            }
            tracer.close();
        }
    }

    @Override
    public void commit(YdbContext ctx, YdbValidator validator) throws SQLException {
        try {
            if (isInsideTransaction() && writing) {
                commitWithCheck(ctx, validator);
            } else {
                super.commit(ctx, validator);
            }
        } finally {
            writing = false;
        }
    }

    @Override
    public void rollback(YdbContext ctx, YdbValidator validator) throws SQLException {
        try {
            super.rollback(ctx, validator);
        } finally {
            writing = false;
        }
    }

    @Override
    public YdbQueryResult executeDataQuery(
            YdbStatement statement, YdbQuery query, String preparedYql, Params params, long timeout, boolean keepInCache
    ) throws SQLException {
        YdbQueryResult yqr = super.executeDataQuery(statement, query, preparedYql, params, timeout, keepInCache);
        if (query.isWriting() && !isAutoCommit) {
            writing = true;
        }
        return yqr;
    }

}
