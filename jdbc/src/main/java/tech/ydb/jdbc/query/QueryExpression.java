package tech.ydb.jdbc.query;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryExpression {
    private final QueryType queryType;
    private final QueryCmd command;
    private final List<String> paramNames = new ArrayList<>();

    public QueryExpression(QueryType type, QueryCmd command) {
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

    public boolean hasResults() {
        return command == QueryCmd.SELECT;
    }

    public boolean isDDL() {
        return command == QueryCmd.CREATE_ALTER_DROP;
    }
}
