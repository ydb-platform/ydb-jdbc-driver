package tech.ydb.jdbc.query.params;



import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private final List<JdbcPrm> parameters = new ArrayList<>();
    private final Map<String, JdbcPrm> parametersByName = new HashMap<>();
    private final List<Params> batchList = new ArrayList<>();

    public InMemoryQuery(YdbQuery query, boolean isAutoDeclare) {
        this.yql = query.getPreparedYql();
        this.isAutoDeclare = isAutoDeclare;

        for (QueryStatement st: query.getStatements()) {
            for (JdbcPrm.Factory factory: st.getJdbcPrmFactories()) {
                for (JdbcPrm prm: factory.create()) {
                    parameters.add(prm);
                    parametersByName.put(prm.getName(), prm);
                }
            }
        }
    }

    @Override
    public String getQueryText(Params prms) {
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
    public String getBatchText(Params prms) {
        return getQueryText(prms);
    }

    @Override
    public int parametersCount() {
        return parameters.size();
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
        if (index <= 0 || index > parameters.size()) {
            throw new SQLDataException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }
        return parameters.get(index - 1).getName();
    }

    @Override
    public TypeDescription getDescription(int index) throws SQLException {
        if (index <= 0 || index > parameters.size()) {
            throw new SQLDataException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }

        JdbcPrm p = parameters.get(index - 1);
        return p.getType();
    }

    @Override
    public void setParam(int index, Object obj, int sqlType) throws SQLException {
        if (index <= 0 || index > parameters.size()) {
            throw new SQLDataException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + index);
        }

        parameters.get(index - 1).setValue(obj, sqlType);
    }

    @Override
    public void setParam(String name, Object obj, int sqlType) throws SQLException {
        JdbcPrm param = parametersByName.get(name);
        if (param == null) {
            throw new SQLDataException(YdbConst.PARAMETER_NUMBER_NOT_FOUND + name);
        }
        param.setValue(obj, sqlType);
    }
}
