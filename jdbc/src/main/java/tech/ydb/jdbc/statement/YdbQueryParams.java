package tech.ydb.jdbc.statement;

import java.util.List;

import javax.annotation.Nullable;

import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbQueryParams {
    void clearParameters();

    void setParam(int index, @Nullable Object obj, @Nullable Type type);
    void setParam(String name, @Nullable Object obj, @Nullable Type type);

    void addBatch();
    void clearBatch();

    int parametersCount();

    int getIndexByName(String name);
    String getNameByIndex(int index);

    TypeDescription getDescription(int index);

    List<Params> toYdbParams();
}
