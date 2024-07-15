package tech.ydb.jdbc.query;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryStatement {
    private final QueryType queryType;
    private final QueryCmd command;
    private final List<String> paramNames = new ArrayList<>();
    private boolean hasReturinng = false;

    public QueryStatement(QueryType type, QueryCmd command) {
        this.queryType = type;
        this.command = command;
    }

    public QueryType getType() {
        return queryType;
    }

    public List<String> getParamNames() {
        return paramNames;
    }

    public void addParamName(String name) {
        this.paramNames.add(name);
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
