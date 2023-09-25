package tech.ydb.jdbc.query;

import java.util.ArrayList;
import java.util.List;

import tech.ydb.core.StatusCode;
import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.exception.YdbNonRetryableException;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryBuilder {
    private final String origin;
    private final StringBuilder query;
    private final List<String> args = new ArrayList<>();
    private final QueryType forcedType;

    private int argsCounter = 0;
    private QueryType currentType = null;

    public YdbQueryBuilder(String origin, QueryType forcedType) {
        this.origin = origin;
        this.query = new StringBuilder(origin.length() + 10);
        this.forcedType = forcedType;
    }

    public String createNextArgName() {
        while (true) {
            argsCounter += 1;
            String next = YdbConst.AUTO_GENERATED_PARAMETER_PREFIX + argsCounter;
            if (!origin.contains(next)) {
                args.add(next);
                return next;
            }
        }
    }

    public void setQueryType(QueryType type) throws YdbNonRetryableException {
        if (forcedType != null) {
            return;
        }

        if (currentType != null && currentType != type) {
            String msg = YdbConst.MULTI_TYPES_IN_ONE_QUERY + currentType + ", " + type;
            throw new YdbNonRetryableException(msg, StatusCode.BAD_REQUEST);
        }
        this.currentType = type;
    }

    public QueryType getQueryType() {
        if (forcedType != null) {
            return forcedType;
        }

        if (currentType != null) {
            return currentType;
        }

        return QueryType.DATA_QUERY;
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
