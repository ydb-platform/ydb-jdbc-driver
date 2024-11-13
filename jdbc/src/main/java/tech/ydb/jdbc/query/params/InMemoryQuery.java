package tech.ydb.jdbc.query.params;



import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.query.QueryStatement;
import tech.ydb.jdbc.query.YdbPreparedQuery;
import tech.ydb.jdbc.query.YdbQuery;
import tech.ydb.table.query.Params;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class InMemoryQuery implements YdbPreparedQuery {
    private final String yql;
    private final boolean isAutoDeclare;
    private final JdbcPrm[] parameters;
    private final Map<String, JdbcPrm> parametersByName;
    private final List<Params> batchList;

    public InMemoryQuery(YdbQuery query, boolean isAutoDeclare) {
        this.yql = query.getPreparedYql();
        this.isAutoDeclare = isAutoDeclare;

        int paramtersCount = 0;
        for (QueryStatement st: query.getStatements()) {
            paramtersCount += st.getParams().size();
        }
        this.parameters = new JdbcPrm[paramtersCount];

        int idx = 0;
        for (QueryStatement st: query.getStatements()) {
            for (Supplier<JdbcPrm> prm: st.getParams()) {
                parameters[idx++] = prm.get();
            }
        }

        this.parametersByName = new HashMap<>();
        for (JdbcPrm prm: this.parameters) {
            parametersByName.put(prm.getName(), prm);
        }

        this.batchList = new ArrayList<>();
    }

    @Override
    public String getQueryText(Params prms) throws SQLException {
        if (!isAutoDeclare) {
            return yql;
        }

        StringBuilder query = new StringBuilder();
        prms.values().forEach((name, value) -> query
                .append("DECLARE ")
                .append(name)
                .append(" AS ")
                .append(value.getType().toString())
                .append(";\n")
        );

        query.append(yql);
        return query.toString();
    }

    @Override
    public int parametersCount() {
        return parameters.length;
    }

    @Override
    public int batchSize() {
        return batchList.size();
    }

    @Override
    public void addBatch() throws SQLException {
        Params batch = Params.create();
        for (JdbcPrm prm: parameters) {
            prm.copyToParams(batch);
            prm.reset();
        }
        batchList.add(batch);
    }

    @Override
    public void clearBatch() {
        batchList.clear();
    }

    @Override
    public void clearParameters() {
        for (JdbcPrm prm: parameters) {
            prm.reset();
        }
    }

    @Override
    public List<Params> getBatchParams() {
        return batchList;
    }

    @Override
    public Params getCurrentParams() throws SQLException {
        Params current = Params.create();
        for (JdbcPrm prm: parameters) {
            prm.copyToParams(current);
        }
        return current;
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

        JdbcPrm p = parameters[index - 1];
        return p.getType();
    }

    @Override
    public void setParam(int index, Object obj, int sqlType) throws SQLException {
        if (index <= 0 || index > parameters.length) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }

        parameters[index - 1].setValue(obj, sqlType);
    }

    @Override
    public void setParam(String name, Object obj, int sqlType) throws SQLException {
        JdbcPrm param = parametersByName.get(name);
        if (param == null) {
            throw new SQLException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + name);
        }
        param.setValue(obj, sqlType);
    }
}
