package tech.ydb.jdbc.query.params;



import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.jdbc.query.JdbcParams;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class InMemoryParams implements JdbcParams {
    private final String[] paramNames;
    private final Map<String, Value<?>> paramValues;
    private final List<Params> batchList;

    public InMemoryParams(List<String> params) {
        this.paramNames = params.toArray(new String[0]);
        this.paramValues = new HashMap<>();
        this.batchList = new ArrayList<>();
    }

    @Override
    public int parametersCount() {
        return paramValues.size();
    }

    @Override
    public int batchSize() {
        return batchList.size();
    }

    @Override
    public void addBatch() {
        batchList.add(Params.copyOf(paramValues));
        paramValues.clear();
    }

    @Override
    public void clearBatch() {
        batchList.clear();
    }

    @Override
    public void clearParameters() {
        paramValues.clear();
    }

    @Override
    public List<Params> getBatchParams() {
        return batchList;
    }

    @Override
    public Params getCurrentParams() {
        return Params.copyOf(paramValues);
    }

    @Override
    public String getNameByIndex(int index) throws SQLException {
        if (index <= 0 || index > paramNames.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        return paramNames[index - 1];
    }

    @Override
    public TypeDescription getDescription(int index) throws SQLException {
        if (index <= 0 || index > paramNames.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        String name = paramNames[index - 1];
        Value<?> arg = paramValues.get(name);
        return arg == null ? null : TypeDescription.of(arg.getType());
    }

    @Override
    public void setParam(int index, Object obj, Type type) throws SQLException {
        if (index <= 0 || index > paramNames.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }

        setParam(paramNames[index - 1], obj, type);
    }

    @Override
    public void setParam(String name, Object obj, Type type) throws SQLException {
        if (obj instanceof Value<?>) {
            paramValues.put(name, (Value<?>)obj);
            return;
        }

        ParamDescription desc = new ParamDescription(-1, name, TypeDescription.of(type));
        paramValues.put(name, desc.getValue(obj));
    }
}
