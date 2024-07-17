package tech.ydb.jdbc.query;

import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbPreparedQuery {
    String getQueryText(Params prms) throws SQLException;

    void clearParameters();

    void setParam(int index, @Nullable Object obj, @Nonnull Type type) throws SQLException;
    void setParam(String name, @Nullable Object obj, @Nonnull Type type) throws SQLException;

    String getNameByIndex(int index) throws SQLException;

    void addBatch() throws SQLException;
    void clearBatch();
    int batchSize();

    int parametersCount();

    TypeDescription getDescription(int index) throws SQLException;

    List<Params> getBatchParams() throws SQLException;
    Params getCurrentParams() throws SQLException;
}
