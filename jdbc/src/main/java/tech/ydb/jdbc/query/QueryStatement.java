package tech.ydb.jdbc.query;

import java.util.ArrayList;
import java.util.List;

import tech.ydb.jdbc.common.TypeDescription;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryStatement {
    private final QueryType queryType;
    private final QueryCmd command;
    private final List<ParamDescription> parameters = new ArrayList<>();
    private boolean hasReturinng = false;

    public QueryStatement(QueryType custom, QueryType baseType, QueryCmd command) {
        this.queryType = custom != null ? custom : baseType;
        this.command = command;
    }

    public QueryType getType() {
        return queryType;
    }

    public QueryCmd getCmd() {
        return command;
    }

    public List<ParamDescription> getParams() {
        return parameters;
    }

    public void addParameter(String name, TypeDescription type) {
        this.parameters.add(new ParamDescription(name, type));
    }

    public void setHasReturning(boolean hasReturning) {
        this.hasReturinng = hasReturning;
    }

    public boolean hasUpdateCount() {
        return (command == QueryCmd.INSERT_UPSERT || command == QueryCmd.UPDATE_REPLACE_DELETE) && !hasReturinng;
    }

    public boolean hasResults() {
        return command == QueryCmd.SELECT || hasReturinng;
    }

    public boolean isDDL() {
        return command == QueryCmd.CREATE_ALTER_DROP;
    }
}
