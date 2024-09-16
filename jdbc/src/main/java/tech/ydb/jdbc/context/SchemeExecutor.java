package tech.ydb.jdbc.context;


import java.util.concurrent.CompletableFuture;

import tech.ydb.core.Result;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SchemeExecutor {
    private final SchemeClient schemeClient;
    private final SessionRetryContext retryCtx;

    public SchemeExecutor(YdbContext ctx) {
        this.schemeClient = ctx.getSchemeClient();
        this.retryCtx = ctx.getRetryCtx();
    }

    public CompletableFuture<Result<ListDirectoryResult>> listDirectory(String path) {
        return schemeClient.listDirectory(path);
    }

    public CompletableFuture<Result<TableDescription>> describeTable(String tablePath, DescribeTableSettings settings) {
        return retryCtx.supplyResult(session -> session.describeTable(tablePath, settings));
    }
}
