package tech.ydb.jdbc.query;


import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

import tech.ydb.jdbc.YdbConst;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryParser {
    private final boolean isDetectQueryType;
    private final boolean isDetectJdbcParameters;

    private final List<QueryStatement> statements = new ArrayList<>();

    public YdbQueryParser(boolean isDetectQueryType, boolean isDetectJdbcParameters) {
        this.isDetectQueryType = isDetectQueryType;
        this.isDetectJdbcParameters = isDetectJdbcParameters;
    }

    public List<QueryStatement> getStatements() {
        return this.statements;
    }

    public QueryType detectQueryType() throws SQLException {
        QueryType type = null;
        for (QueryStatement st: statements) {
            if (st.getType() == QueryType.UNKNOWN) {
                continue;
            }

            if (type == null) {
                type = st.getType();
            } else {
                if (type != st.getType()) {
                    String msg = YdbConst.MULTI_TYPES_IN_ONE_QUERY + type + ", " + st.getType();
                    throw new SQLFeatureNotSupportedException(msg);
                }
            }
        }

        return type != null ? type : QueryType.DATA_QUERY;
    }

    public String parseSQL(String origin) throws SQLException {
        this.statements.clear();

        int fragmentStart = 0;

        boolean detectJdbcArgs = false;

        QueryStatement currStatement = null;

        int parenLevel = 0;
        int keywordStart = -1;

        char[] chars = origin.toCharArray();

        StringBuilder parsed = new StringBuilder(origin.length() + 10);
        ArgNameGenerator argNameGenerator = new ArgNameGenerator();

        for (int i = 0; i < chars.length; ++i) {
            char ch = chars[i];
            boolean isInsideKeyword = false;
            switch (ch) {
                case '(':
                    parenLevel++;
                    break;

                case ')':
                    parenLevel--;
                    break;

                case '\'': // single-quotes
                    i = parseSingleQuotes(chars, i);
                    break;

                case '"': // double-quotes
                    i = parseDoubleQuotes(chars, i);
                    break;

                case '-': // possibly -- style comment
                    i = parseLineComment(chars, i);
                    break;

                case '/': // possibly /* */ style comment
                    i = parseBlockComment(chars, i);
                    break;
                case '?':
                    if (detectJdbcArgs && currStatement != null) {
                        parsed.append(chars, fragmentStart, i - fragmentStart);
                        if (i + 1 < chars.length && chars[i + 1] == '?') /* replace ?? with ? */ {
                            parsed.append('?');
                            i++; // make sure the coming ? is not treated as a bind
                        } else {
                            String binded = argNameGenerator.createArgName(origin);
                            currStatement.addParameter(binded, null);
                            parsed.append(binded);
                        }
                        fragmentStart = i + 1;
                    }
                    break;
                default:
                    if (keywordStart >= 0) {
                        isInsideKeyword = Character.isJavaIdentifierPart(ch);
                        break;
                    }
                    // Not in keyword, so just detect next keyword start
                    isInsideKeyword = Character.isJavaIdentifierStart(ch);
                    if (isInsideKeyword) {
                        keywordStart = i;
                    }
                    break;
            }

            if (keywordStart >= 0 && (!isInsideKeyword || (i == chars.length - 1))) {

                if (currStatement != null) {
                    // Detect RETURNING keyword
                    if (parenLevel == 0 && parseReturningKeyword(chars, keywordStart)) {
                        currStatement.setHasReturning(true);
                    }
                } else {
                    // Detecting type of statement by the first keyword
                    currStatement = new QueryStatement(QueryType.UNKNOWN, QueryCmd.UNKNOWN);
                    // Detect data query expression - starts with SELECT, , UPSERT, DELETE, REPLACE
                    // starts with SELECT
                    if (parseSelectKeyword(chars, keywordStart)) {
                        currStatement = new QueryStatement(QueryType.DATA_QUERY, QueryCmd.SELECT);
                    }
                    // starts with INSERT, UPSERT
                    if (parseInsertKeyword(chars, keywordStart)
                            || parseUpsertKeyword(chars, keywordStart)) {
                        currStatement = new QueryStatement(QueryType.DATA_QUERY, QueryCmd.INSERT_UPSERT);
                    }
                    // starts with UPDATE, REPLACE, DELETE
                    if (parseUpdateKeyword(chars, keywordStart)
                            || parseDeleteKeyword(chars, keywordStart)
                            || parseReplaceKeyword(chars, keywordStart)) {
                        currStatement = new QueryStatement(QueryType.DATA_QUERY, QueryCmd.UPDATE_REPLACE_DELETE);
                    }

                    // Detect scheme expression - starts with ALTER, DROP, CREATE
                    if (parseAlterKeyword(chars, keywordStart)
                            || parseCreateKeyword(chars, keywordStart)
                            || parseDropKeyword(chars, keywordStart)) {
                        currStatement = new QueryStatement(QueryType.SCHEME_QUERY, QueryCmd.CREATE_ALTER_DROP);
                    }
                    if (isDetectQueryType) {
                        // Detect scan expression - starts with SCAN
                        if (parseScanKeyword(chars, keywordStart)) {
                            currStatement = new QueryStatement(QueryType.SCAN_QUERY, QueryCmd.SELECT);
                            // Skip SCAN prefix
                            parsed.append(chars, fragmentStart, keywordStart - fragmentStart);
                            fragmentStart = isInsideKeyword ? i + 1 : i;
                        }
                        // Detect explain expression - starts with EXPLAIN
                        if (parseExplainKeyword(chars, keywordStart)) {
                            currStatement = new QueryStatement(QueryType.EXPLAIN_QUERY, QueryCmd.SELECT);
                            // Skip EXPLAIN prefix
                            parsed.append(chars, fragmentStart, keywordStart - fragmentStart);
                            fragmentStart = isInsideKeyword ? i + 1 : i;
                        }
                    }

                    statements.add(currStatement);
                    detectJdbcArgs = currStatement.getType() != QueryType.SCHEME_QUERY
                            && currStatement.getType() != QueryType.UNKNOWN
                            && isDetectJdbcParameters;

                    if (parseAlterKeyword(chars, i)
                            || parseCreateKeyword(chars, i)
                            || parseDropKeyword(chars, i)) {
                        currStatement = new QueryStatement(QueryType.SCHEME_QUERY, QueryCmd.CREATE_ALTER_DROP);
                        statements.add(currStatement);
                        break;
                    }
                }

                keywordStart = -1;
            }

            if (ch == ';' && parenLevel == 0) {
                currStatement = null;
                detectJdbcArgs = false;
            }
        }

        if (fragmentStart < chars.length) {
            parsed.append(chars, fragmentStart, chars.length - fragmentStart);
        }

        return parsed.toString();
    }

    private static class ArgNameGenerator {
        private int index = 0;

        public String createArgName(String origin) {
            while (true) {
                index += 1;
                String name = YdbConst.AUTO_GENERATED_PARAMETER_PREFIX + index;
                if (!origin.contains(name)) {
                    return name;
                }
            }
        }
    }

    private static int parseSingleQuotes(final char[] query, int offset) {
        // treat backslashes as escape characters
        while (++offset < query.length) {
            switch (query[offset]) {
                case '\\':
                    ++offset;
                    break;
                case '\'':
                    return offset;
                default:
                    break;
            }
        }

        return query.length;
    }

    @SuppressWarnings("EmptyBlock")
    private static int parseDoubleQuotes(final char[] query, int offset) {
        while (++offset < query.length && query[offset] != '"') {
            // do nothing
        }
        return offset;
    }

    private static int parseLineComment(final char[] query, int offset) {
        if (offset + 1 < query.length && query[offset + 1] == '-') {
            while (offset + 1 < query.length) {
                offset++;
                if (query[offset] == '\r' || query[offset] == '\n') {
                    break;
                }
            }
        }
        return offset;
    }

    private static int parseBlockComment(final char[] query, int offset) {
        if (offset + 1 < query.length && query[offset + 1] == '*') {
            // /* /* */ */ nest, according to SQL spec
            int level = 1;
            for (offset += 2; offset < query.length; ++offset) {
                switch (query[offset - 1]) {
                    case '*':
                        if (query[offset] == '/') {
                            --level;
                            ++offset; // don't parse / in */* twice
                        }
                        break;
                    case '/':
                        if (query[offset] == '*') {
                            ++level;
                            ++offset; // don't parse * in /*/ twice
                        }
                        break;
                    default:
                        break;
                }

                if (level == 0) {
                    --offset; // reset position to last '/' char
                    break;
                }
            }
        }
        return offset;
    }

    private static boolean isSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f';
    }

    private static boolean parseAlterKeyword(char[] query, int offset) {
        if (query.length < (offset + 6)) {
            return false;
        }

        return (query[offset] | 32) == 'a'
                && (query[offset + 1] | 32) == 'l'
                && (query[offset + 2] | 32) == 't'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && isSpace(query[offset + 5]);
    }

    private static boolean parseCreateKeyword(char[] query, int offset) {
        if (query.length < (offset + 6)) {
            return false;
        }

        return (query[offset] | 32) == 'c'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'e'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseDropKeyword(char[] query, int offset) {
        if (query.length < (offset + 4)) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'o'
                && (query[offset + 3] | 32) == 'p';
    }

    private static boolean parseScanKeyword(char[] query, int offset) {
        if (query.length < (offset + 4)) {
            return false;
        }

        return (query[offset] | 32) == 's'
                && (query[offset + 1] | 32) == 'c'
                && (query[offset + 2] | 32) == 'a'
                && (query[offset + 3] | 32) == 'n';
    }

    private static boolean parseExplainKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'e'
                && (query[offset + 1] | 32) == 'x'
                && (query[offset + 2] | 32) == 'p'
                && (query[offset + 3] | 32) == 'l'
                && (query[offset + 4] | 32) == 'a'
                && (query[offset + 5] | 32) == 'i'
                && (query[offset + 6] | 32) == 'n';
    }

    private static boolean parseSelectKeyword(char[] query, int offset) {
        if (query.length < (offset + 6)) {
            return false;
        }

        return (query[offset] | 32) == 's'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'c'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseUpdateKeyword(char[] query, int offset) {
        if (query.length < (offset + 6)) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 'd'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseUpsertKeyword(char[] query, int offset) {
        if (query.length < (offset + 6)) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseInsertKeyword(char[] query, int offset) {
        if (query.length < (offset + 6)) {
            return false;
        }

        return (query[offset] | 32) == 'i'
                && (query[offset + 1] | 32) == 'n'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseDeleteKeyword(char[] query, int offset) {
        if (query.length < (offset + 6)) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseReplaceKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'r'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'p'
                && (query[offset + 3] | 32) == 'l'
                && (query[offset + 4] | 32) == 'a'
                && (query[offset + 5] | 32) == 'c'
                && (query[offset + 6] | 32) == 'e';
    }

    private static boolean parseReturningKeyword(char[] query, int offset) {
        if (query.length < (offset + 9)) {
            return false;
        }

        return (query[offset] | 32) == 'r'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 't'
                && (query[offset + 3] | 32) == 'u'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 'n'
                && (query[offset + 6] | 32) == 'i'
                && (query[offset + 7] | 32) == 'n'
                && (query[offset + 8] | 32) == 'g';
    }
}
