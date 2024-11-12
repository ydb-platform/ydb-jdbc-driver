package tech.ydb.jdbc.query.params;



import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.impl.YdbTypes;
import tech.ydb.jdbc.query.YdbPreparedQuery;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class InMemoryQuery implements YdbPreparedQuery {
    private final String yql;
    private final boolean isAutoDeclare;
    private final JdbcParameter[] parameters;
    private final Map<String, JdbcParameter> parametersByName;
    private final Map<String, Value<?>> paramValues;
    private final List<Params> batchList;

    public InMemoryQuery(YdbQuery query, boolean isAutoDeclare) {
        this.yql = query.getPreparedYql();
        this.isAutoDeclare = isAutoDeclare;
        this.parameters = query.getStatements().stream()
                .flatMap(s -> s.getParams().stream())
                .toArray(JdbcParameter[]::new);
        this.parametersByName = query.getStatements().stream()
                .flatMap(s -> s.getParams().stream())
                .collect(Collectors.toMap(JdbcParameter::getName, Function.identity()));

        this.paramValues = new HashMap<>();
        this.batchList = new ArrayList<>();
    }

    @Override
    public String getQueryText(Params prms) throws SQLException {
        if (!isAutoDeclare) {
            return yql;
        }

        StringBuilder query = new StringBuilder();
        Map<String, Value<?>> values = prms.values();
        for (JdbcParameter prm: parameters) {
            query.append(prm.getDeclare(values));
        }

        query.append(yql);
        return query.toString();
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
        if (index <= 0 || index > parameters.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        return parameters[index - 1].getName();
    }

    @Override
    public TypeDescription getDescription(int index) throws SQLException {
        if (index <= 0 || index > parameters.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        JdbcParameter p = parameters[index - 1];
        if (p.getForcedType() != null) {
            return p.getForcedType();
        }

        Value<?> arg = paramValues.get(p.getName());
        if (arg != null) {
            return TypeDescription.of(arg.getType());
        }

        return null;
    }

    @Override
    public void setParam(int index, Object obj, int sqlType) throws SQLException {
        if (index <= 0 || index > parameters.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }

        setParam(parameters[index - 1], obj, sqlType);
    }

    @Override
    public void setParam(String name, Object obj, int sqlType) throws SQLException {
        JdbcParameter param = parametersByName.get(name);
        if (param == null) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + name);
        }
        setParam(param, obj, sqlType);
    }

    private void setParam(JdbcParameter param, Object obj, int sqlType) throws SQLException {
        if (obj instanceof Value<?>) {
            paramValues.put(param.getName(), (Value<?>) obj);
            return;
        }

        TypeDescription description = param.getForcedType();
        if (description == null) {
            Type type = YdbTypes.findType(obj, sqlType);
            if (type == null) {
                throw new SQLException(String.format(YdbConst.PARAMETER_TYPE_UNKNOWN, sqlType, obj));
            }
            description = TypeDescription.of(type);
        }

        paramValues.put(param.getName(), ValueFactory.readValue(param.getName(), obj, description));
    }
}
