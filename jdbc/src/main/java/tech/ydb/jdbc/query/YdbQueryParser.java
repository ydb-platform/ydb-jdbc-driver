package tech.ydb.jdbc.query;


import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.table.values.PrimitiveType;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryParser {
    private final boolean isDetectQueryType;
    private final boolean isDetectJdbcParameters;

    private final List<QueryStatement> statements = new ArrayList<>();
    private final YqlBatcher batcher = new YqlBatcher();

    public YdbQueryParser(boolean isDetectQueryType, boolean isDetectJdbcParameters) {
        this.isDetectQueryType = isDetectQueryType;
        this.isDetectJdbcParameters = isDetectJdbcParameters;
    }

    public List<QueryStatement> getStatements() {
        return this.statements;
    }

    public YqlBatcher getYqlBatcher() {
        return this.batcher;
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

    @SuppressWarnings("MethodLength")
    public String parseSQL(String origin) throws SQLException {
        int fragmentStart = 0;
        boolean detectJdbcArgs = false;

        QueryStatement statement = null;
        QueryType type = null;

        int parenLevel = 0;
        int keywordStart = -1;
        boolean lastKeywordIsOffsetLimit = false;

        char[] chars = origin.toCharArray();

        StringBuilder parsed = new StringBuilder(origin.length() + 10);
        ArgNameGenerator argNameGenerator = new ArgNameGenerator();

        for (int i = 0; i < chars.length; ++i) {
            char ch = chars[i];
            boolean isInsideKeyword = false;
            int keywordEnd = i; // parseSingleQuotes, parseDoubleQuotes, etc move index so we keep old value
            switch (ch) {
                case '\'': // single-quotes
                    int singleQuitesEnd = parseSingleQuotes(chars, i);
                    batcher.readSingleQuoteLiteral(chars, i, singleQuitesEnd - i + 1);
                    i = singleQuitesEnd;
                    break;

                case '"': // double-quotes
                    int doubleQuitesEnd = parseDoubleQuotes(chars, i);
                    batcher.readDoubleQuoteLiteral(chars, i, doubleQuitesEnd - i + 1);
                    i = doubleQuitesEnd;
                    break;

                case '`': // backtick-quotes
                    int backstickQuitesEnd = parseBacktickQuotes(chars, i);
                    batcher.readIdentifier(chars, i, backstickQuitesEnd - i + 1);
                    i = backstickQuitesEnd;
                    break;

                case '-': // possibly -- style comment
                    i = parseLineComment(chars, i);
                    break;

                case '/': // possibly /* */ style comment
                    i = parseBlockComment(chars, i);
                    break;
                case '?':
                    if (detectJdbcArgs && statement != null) {
                        parsed.append(chars, fragmentStart, i - fragmentStart);
                        if (i + 1 < chars.length && chars[i + 1] == '?') /* replace ?? with ? */ {
                            parsed.append('?');
                            batcher.readIdentifier(chars, i, 1);
                            i++; // make sure the coming ? is not treated as a bind
                        } else {
                            String binded = argNameGenerator.createArgName(origin);
                            // force type UInt64 for OFFSET and LIMIT parameters
                            TypeDescription forcedType = lastKeywordIsOffsetLimit
                                    ? TypeDescription.of(PrimitiveType.Uint64)
                                    : null;
                            statement.addParameter(binded, forcedType);
                            parsed.append(binded);

                            batcher.readParameter();
                        }
                        fragmentStart = i + 1;
                    }
                    break;
                default:
                    lastKeywordIsOffsetLimit = lastKeywordIsOffsetLimit && Character.isWhitespace(ch);

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
                lastKeywordIsOffsetLimit = false;
                int keywordLength = (isInsideKeyword ? i + 1 : keywordEnd) - keywordStart;

                if (statement != null) {
                    batcher.readIdentifier(chars, keywordStart, keywordLength);

                    // Detect RETURNING keyword
                    if (parenLevel == 0 && parseReturningKeyword(chars, keywordStart, keywordLength)) {
                        statement.setHasReturning(true);
                    }

                    if (parseOffsetKeyword(chars, keywordStart, keywordLength)
                            || parseLimitKeyword(chars, keywordStart, keywordLength)) {
                        lastKeywordIsOffsetLimit = Character.isWhitespace(ch);
                    }
                } else {
                    boolean skipped = false;
                    if (isDetectQueryType) {
                        // Detect scan expression - starts with SCAN
                        if (parseScanKeyword(chars, keywordStart, keywordLength)) {
                            type = QueryType.SCAN_QUERY;
                            // Skip SCAN prefix
                            parsed.append(chars, fragmentStart, keywordStart - fragmentStart);
                            fragmentStart = isInsideKeyword ? keywordEnd + 1 : keywordEnd;
                            skipped = true;
                        }
                        // Detect explain expression - starts with EXPLAIN
                        if (parseExplainKeyword(chars, keywordStart, keywordLength)) {
                            type = QueryType.EXPLAIN_QUERY;
                            // Skip EXPLAIN prefix
                            parsed.append(chars, fragmentStart, keywordStart - fragmentStart);
                            fragmentStart = isInsideKeyword ? keywordEnd + 1 : keywordEnd;
                            skipped = true;
                        }
                        // Detect bulk upsert expression - starts with BULK
                        if (parseBulkKeyword(chars, keywordStart, keywordLength)) {
                            type = QueryType.BULK_QUERY;
                            // Skip BULK prefix
                            parsed.append(chars, fragmentStart, keywordStart - fragmentStart);
                            fragmentStart = isInsideKeyword ? keywordEnd + 1 : keywordEnd;
                            skipped = true;
                        }
                    }

                    if (!skipped) {
                        // Detecting type of statement by the first keyword
                        statement = new QueryStatement(type, QueryType.UNKNOWN, QueryCmd.UNKNOWN);
                        // Detect data query expression - starts with SELECT, , UPSERT, DELETE, REPLACE
                        // starts with SELECT
                        if (parseSelectKeyword(chars, keywordStart, keywordLength)) {
                            statement = new QueryStatement(type, QueryType.DATA_QUERY, QueryCmd.SELECT);
                            batcher.readIdentifier(chars, keywordStart, keywordLength);
                        }

                        // starts with INSERT, UPSERT
                        if (parseInsertKeyword(chars, keywordStart, keywordLength)) {
                            statement = new QueryStatement(type, QueryType.DATA_QUERY, QueryCmd.INSERT_UPSERT);
                            batcher.readInsert();
                        }
                        if (parseUpsertKeyword(chars, keywordStart, keywordLength)) {
                            statement = new QueryStatement(type, QueryType.DATA_QUERY, QueryCmd.INSERT_UPSERT);
                            batcher.readUpsert();
                        }

                        // starts with UPDATE, REPLACE, DELETE
                        if (parseUpdateKeyword(chars, keywordStart, keywordLength)
                                || parseDeleteKeyword(chars, keywordStart, keywordLength)
                                || parseReplaceKeyword(chars, keywordStart, keywordLength)) {
                            statement = new QueryStatement(type, QueryType.DATA_QUERY, QueryCmd.UPDATE_REPLACE_DELETE);
                            batcher.readIdentifier(chars, keywordStart, keywordLength);
                        }

                        // Detect scheme expression - starts with ALTER, DROP, CREATE
                        if (parseAlterKeyword(chars, keywordStart, keywordLength)
                                || parseCreateKeyword(chars, keywordStart, keywordLength)
                                || parseDropKeyword(chars, keywordStart, keywordLength)) {
                            statement = new QueryStatement(type, QueryType.SCHEME_QUERY, QueryCmd.CREATE_ALTER_DROP);
                            batcher.readIdentifier(chars, keywordStart, keywordLength);
                        }

                        statements.add(statement);
                        detectJdbcArgs = statement.getType() != QueryType.SCHEME_QUERY
                                && statement.getType() != QueryType.UNKNOWN
                                && isDetectJdbcParameters;
                    }
                }

                keywordStart = -1;
            }

            switch (ch) {
                case '(':
                    parenLevel++;
                    batcher.readOpenParen();
                    break;
                case ')':
                    parenLevel--;
                    batcher.readCloseParen();
                    break;
                case ',':
                    batcher.readComma();
                    break;
                case ';':
                    batcher.readSemiColon();
                    if (parenLevel == 0) {
                        statement = null;
                        type = null;
                        detectJdbcArgs = false;
                    }
                    break;
                default:
                    // nothing
                    break;
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

    @SuppressWarnings("EmptyBlock")
    private static int parseBacktickQuotes(final char[] query, int offset) {
        while (++offset < query.length && query[offset] != '`') {
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

    private static boolean parseAlterKeyword(char[] query, int offset, int length) {
        if (length != 5) {
            return false;
        }

        return (query[offset] | 32) == 'a'
                && (query[offset + 1] | 32) == 'l'
                && (query[offset + 2] | 32) == 't'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r';
    }

    private static boolean parseCreateKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'c'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'e'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseDropKeyword(char[] query, int offset, int length) {
        if (length != 4) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'o'
                && (query[offset + 3] | 32) == 'p';
    }

    private static boolean parseScanKeyword(char[] query, int offset, int length) {
        if (length != 4) {
            return false;
        }

        return (query[offset] | 32) == 's'
                && (query[offset + 1] | 32) == 'c'
                && (query[offset + 2] | 32) == 'a'
                && (query[offset + 3] | 32) == 'n';
    }

    private static boolean parseBulkKeyword(char[] query, int offset, int length) {
        if (length != 4) {
            return false;
        }

        return (query[offset] | 32) == 'b'
                && (query[offset + 1] | 32) == 'u'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'k';
    }

    private static boolean parseExplainKeyword(char[] query, int offset, int length) {
        if (length != 7) {
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

    private static boolean parseSelectKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 's'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'c'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseUpdateKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 'd'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseUpsertKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseInsertKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'i'
                && (query[offset + 1] | 32) == 'n'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseDeleteKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e';
    }

    private static boolean parseReplaceKeyword(char[] query, int offset, int length) {
        if (length != 7) {
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

    private static boolean parseReturningKeyword(char[] query, int offset, int length) {
        if (length != 9) {
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

    private static boolean parseOffsetKeyword(char[] query, int offset, int length) {
        if (length != 6) {
            return false;
        }

        return (query[offset] | 32) == 'o'
                && (query[offset + 1] | 32) == 'f'
                && (query[offset + 2] | 32) == 'f'
                && (query[offset + 3] | 32) == 's'
                && (query[offset + 4] | 32) == 'e'
                && (query[offset + 5] | 32) == 't';
    }

    private static boolean parseLimitKeyword(char[] query, int offset, int length) {
        if (length != 5) {
            return false;
        }

        return (query[offset] | 32) == 'l'
                && (query[offset + 1] | 32) == 'i'
                && (query[offset + 2] | 32) == 'm'
                && (query[offset + 3] | 32) == 'i'
                && (query[offset + 4] | 32) == 't';
    }
}
