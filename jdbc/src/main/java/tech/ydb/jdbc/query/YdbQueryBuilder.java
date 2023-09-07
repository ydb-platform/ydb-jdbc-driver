package tech.ydb.jdbc.query;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryBuilder {
    private final static String JDBC_ARG_PREFIX = "$jp";

    private final String origin;
    private final StringBuilder query;
    private final List<String> args = new ArrayList<>();

    private int argsCounter = 0;
    private QueryType currentType = null;

    public YdbQueryBuilder(String origin) {
        this.origin = origin;
        this.query = new StringBuilder(origin.length() + 10);
    }

    public String createNextArgName() {
        while (true) {
            argsCounter += 1;
            String next = JDBC_ARG_PREFIX + argsCounter;
            if (!origin.contains(next)) {
                args.add(next);
                return next;
            }
        }
    }

    public void setQueryType(QueryType type) {
        this.currentType = type;
    }

    public QueryType getQueryType() {
        return currentType != null ? currentType : QueryType.DATA_QUERY;
    }

    public String getOriginSQL() {
        return origin;
    }

    public String buildYQL() {
        return query.toString();
    }

    public List<String> getIndexedArgs() {
        return args;
    }

    public void append(char[] chars, int start, int end) {
        query.append(chars, start, end);
    }

    public void append(char ch) {
        query.append(ch);
    }

    public void append(String string) {
        query.append(string);
    }
}
