package tech.ydb.jdbc.impl.params;



import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.impl.YdbJdbcParams;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class InMemoryParams implements YdbJdbcParams {
    private final String[] paramNames;
    private final Map<String, Integer> paramIndexes;
    private final Value<?>[] paramValues;
    private final List<Params> batchList;

    public InMemoryParams(List<String> params) {
        this.paramNames = params.toArray(new String[0]);
        this.paramIndexes = new HashMap<>();
        for (int idx = 0; idx < paramNames.length; idx += 1) {
            paramIndexes.put(paramNames[idx], idx);
        }
        this.paramValues = new Value<?>[paramNames.length];
        this.batchList = new ArrayList<>();
    }

    @Override
    public int parametersCount() {
        return paramValues.length;
    }

    @Override
    public int batchSize() {
        return batchList.size();
    }

    @Override
    public void addBatch() {
        Params batch = Params.create();
        for (int idx = 0; idx < paramValues.length; idx += 1) {
            if (paramValues[idx] != null) {
                batch.put(paramNames[idx], paramValues[idx]);
                paramValues[idx] = null;
            }
        }
        batchList.add(batch);
    }

    @Override
    public void clearBatch() {
        batchList.clear();
    }

    @Override
    public void clearParameters() {
        Arrays.fill(paramValues, null);
    }

    @Override
    public List<Params> getBatchParams() {
        return batchList;
    }

    @Override
    public Params getCurrentParams() {
        Params batch = Params.create();
        for (int idx = 0; idx < paramValues.length; idx += 1) {
            if (paramValues[idx] != null) {
                batch.put(paramNames[idx], paramValues[idx]);
            }
        }
        return batch;
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
        if (index <= 0 || index > paramValues.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        Value<?> arg = paramValues[index - 1];
        return arg == null ? null : TypeDescription.of(arg.getType());
    }

    @Override
    public void setParam(int index, Object obj, Type type) throws SQLException {
        if (index <= 0 || index > paramValues.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }

        if (obj instanceof Value<?>) {
            paramValues[index - 1] = (Value<?>)obj;
            return;
        }

        ParamDescription desc = new ParamDescription(index - 1, paramNames[index - 1], TypeDescription.of(type));
        paramValues[index - 1] = desc.getValue(obj);
    }

    @Override
    public void setParam(String name, Object obj, Type type) throws SQLException {
        if (!paramIndexes.containsKey(name)) {
            throw new SQLException(YdbConst.PARAMETER_NOT_FOUND + name);
        }
        setParam(paramIndexes.get(name), obj, type);
    }
}
