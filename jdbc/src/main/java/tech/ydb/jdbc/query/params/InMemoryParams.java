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
import tech.ydb.jdbc.query.ParamDescription;
import tech.ydb.jdbc.query.QueryStatement;
import tech.ydb.jdbc.query.YdbPreparedParams;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class InMemoryParams implements YdbPreparedParams {
    private final ParamDescription[] parameters;
    private final Map<String, ParamDescription> parametersByName;
    private final Map<String, Value<?>> paramValues;
    private final List<Params> batchList;

    public InMemoryParams(List<QueryStatement> statements) {
        this.parameters = statements.stream()
                .flatMap(s -> s.getParams().stream())
                .toArray(ParamDescription[]::new);
        this.parametersByName = statements.stream()
                .flatMap(s -> s.getParams().stream())
                .collect(Collectors.toMap(ParamDescription::name, Function.identity()));

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
        if (index <= 0 || index > parameters.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        return parameters[index - 1].name();
    }

    @Override
    public TypeDescription getDescription(int index) throws SQLException {
        if (index <= 0 || index > parameters.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        ParamDescription p = parameters[index - 1];
        if (p.type() != null) {
            return p.type();
        }

        Value<?> arg = paramValues.get(p.name());
        if (arg != null) {
            return TypeDescription.of(arg.getType());
        }

        return null;
    }

    @Override
    public void setParam(int index, Object obj, Type type) throws SQLException {
        if (index <= 0 || index > parameters.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }

        setParam(parameters[index - 1], obj, type);
    }

    @Override
    public void setParam(String name, Object obj, Type type) throws SQLException {
        ParamDescription param = parametersByName.get(name);
        if (param == null) {
            param = new ParamDescription(name, null);
        }
        setParam(param, obj, type);
    }

    private void setParam(ParamDescription param, Object obj, Type type) throws SQLException {
        if (obj instanceof Value<?>) {
            paramValues.put(param.name(), (Value<?>) obj);
            return;
        }

        TypeDescription description = param.type();
        if (description == null) {
            description = TypeDescription.of(type);
        }

        paramValues.put(param.name(), ValueFactory.readValue(param.name(), obj, description));
    }
}
