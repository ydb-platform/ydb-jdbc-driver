package tech.ydb.jdbc.impl.params;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
public class PreparedParams implements YdbJdbcParams {
    private final Map<String, ParamDescription> params;
    private final String[] paramNames;

    private final Map<String, Value<?>> paramValues = new HashMap<>();
    private final List<Params> batchList = new ArrayList<>();

    public PreparedParams(Map<String, Type> types) {
        params = new HashMap<>();
        paramNames = new String[types.size()];

        // Firstly put all indexed params (p1, p2, ...,  pN) in correct places of paramNames
        Set<String> indexedNames = new HashSet<>();
        for (int idx = 0; idx < paramNames.length; idx += 1) {
            String indexedName = YdbConst.VARIABLE_PARAMETER_PREFIX + YdbConst.INDEXED_PARAMETER_PREFIX + (1 + idx);
            if (types.containsKey(indexedName)) {
                TypeDescription typeDesc = TypeDescription.of(types.get(indexedName));
                ParamDescription paramDesc = new ParamDescription(idx, indexedName, typeDesc);

                params.put(indexedName, paramDesc);
                paramNames[idx] = indexedName;
                indexedNames.add(indexedName);
            }
        }

        // Then put all others params in free places of paramNames in alphabetic order
        Iterator<String> sortedIter = new TreeSet<>(types.keySet()).iterator();
        for (int idx = 0; idx < paramNames.length; idx += 1) {
            if (paramNames[idx] != null) {
                continue;
            }

            String param = sortedIter.next();
            while (indexedNames.contains(param)) {
                param = sortedIter.next();
            }

            TypeDescription typeDesc = TypeDescription.of(types.get(param));
            ParamDescription paramDesc = new ParamDescription(idx, param, typeDesc);

            params.put(param, paramDesc);
            paramNames[idx] = param;
        }
    }

    @Override
    public void setParam(int index, Object obj, Type type) throws SQLException {
        if (index <= 0 || index > paramNames.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        String varName = paramNames[index - 1];
        ParamDescription desc = params.get(varName);
        paramValues.put(varName, desc.getValue(obj));
    }

    @Override
    public void setParam(String name, Object obj, Type type) throws SQLException {
        String varName = YdbConst.VARIABLE_PARAMETER_PREFIX + name;
        if (!params.containsKey(varName)) {
            throw new SQLException(YdbConst.PARAMETER_NOT_FOUND + name);
        }

        ParamDescription desc = params.get(varName);
        paramValues.put(varName, desc.getValue(obj));
    }

    @Override
    public void clearParameters() {
        paramValues.clear();
    }

    @Override
    public void addBatch() {
        batchList.add(getCurrentParams());
        clearParameters();
    }

    @Override
    public void clearBatch() {
        batchList.clear();
    }

    @Override
    public int parametersCount() {
        return params.size();
    }

    @Override
    public int batchSize() {
        return batchList.size();
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
        return params.get(name).type();
    }
}
