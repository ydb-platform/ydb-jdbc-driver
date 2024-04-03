package tech.ydb.jdbc.query;


import java.sql.SQLException;

import tech.ydb.jdbc.settings.YdbQueryProperties;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class JdbcQueryLexer {
    private JdbcQueryLexer() { }

    /**
     * Parses JDBC query to replace all ? to YQL parameters.
     *
     * @param builder Ydb query builder
     * @param options Options of parsing
     * @throws java.sql.SQLException if query contains mix of query types
     */
    public static void buildQuery(YdbQueryBuilder builder, YdbQueryProperties options) throws SQLException {
        int fragmentStart = 0;

        boolean nextExpression = true;
        boolean detectJdbcArgs = false;

        char[] chars = builder.getOriginSQL().toCharArray();

        for (int i = 0; i < chars.length; ++i) {
            char ch = chars[i];
            switch (ch) {
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
                case ';': // next chars will be new expression
                    nextExpression = true;
                    detectJdbcArgs = false;
                    break;
                case '?':
                    if (detectJdbcArgs) {
                        builder.append(chars, fragmentStart, i - fragmentStart);
                        if (i + 1 < chars.length && chars[i + 1] == '?') /* replace ?? with ? */ {
                            builder.append('?');
                            i++; // make sure the coming ? is not treated as a bind
                        } else {
                            String binded = builder.createNextArgName();
                            builder.append(binded);
                        }
                        fragmentStart = i + 1;
                    }
                    break;
                default:
                    if (nextExpression && Character.isJavaIdentifierStart(ch)) {
                        nextExpression = false;

                        if (!options.isDetectQueryType()) {
                            break;
                        }

                        // Detect data query expression - starts with SELECT, UPDATE, INSERT, UPSERT, DELETE, REPLACE
                        if (parseSelectKeyword(chars, i)) {
                            builder.addExpression(QueryType.DATA_QUERY, YdbExpression.SELECT);
                            detectJdbcArgs = options.isDetectJdbcParameters();
                            break;
                        }

                        if (parseUpdateKeyword(chars, i)
                                || parseInsertKeyword(chars, i)
                                || parseUpsertKeyword(chars, i)
                                || parseDeleteKeyword(chars, i)
                                || parseReplaceKeyword(chars, i)
                                ) {
                            builder.addExpression(QueryType.DATA_QUERY, YdbExpression.OTHER_DML);
                            detectJdbcArgs = options.isDetectJdbcParameters();
                            break;
                        }

                        // Detect scheme expression - starts with ALTER, DROP, CREATE
                        if (parseAlterKeyword(chars, i)
                                || parseCreateKeyword(chars, i)
                                || parseDropKeyword(chars, i)) {
                            builder.addExpression(QueryType.SCHEME_QUERY, YdbExpression.DDL);
                            break;
                        }

                        // Detect scan expression - starts with SCAN
                        if (parseScanKeyword(chars, i)) {
                            builder.addExpression(QueryType.SCAN_QUERY, YdbExpression.SELECT);
                            detectJdbcArgs = options.isDetectJdbcParameters();

                            // Skip SCAN prefix
                            builder.append(chars, fragmentStart, i - fragmentStart);
                            fragmentStart = i + 5;
                            break;
                        }

                        // Detect explain expression - starts with EXPLAIN
                        if (parseExplainKeyword(chars, i)) {
                            builder.addExpression(QueryType.EXPLAIN_QUERY, YdbExpression.SELECT);
                            detectJdbcArgs = options.isDetectJdbcParameters();

                            // Skip EXPLAIN prefix
                            builder.append(chars, fragmentStart, i - fragmentStart);
                            fragmentStart = i + 8;
                            break;
                        }
                    }
                    break;
            }
        }


        if (fragmentStart < chars.length) {
            builder.append(chars, fragmentStart, chars.length - fragmentStart);
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
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'c'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'e'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e'
                && isSpace(query[offset + 6]);
    }

    private static boolean parseDropKeyword(char[] query, int offset) {
        if (query.length < (offset + 5)) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'o'
                && (query[offset + 3] | 32) == 'p'
                && isSpace(query[offset + 4]);
    }

    private static boolean parseScanKeyword(char[] query, int offset) {
        if (query.length < (offset + 5)) {
            return false;
        }

        return (query[offset] | 32) == 's'
                && (query[offset + 1] | 32) == 'c'
                && (query[offset + 2] | 32) == 'a'
                && (query[offset + 3] | 32) == 'n'
                && isSpace(query[offset + 4]);
    }

    private static boolean parseExplainKeyword(char[] query, int offset) {
        if (query.length < (offset + 8)) {
            return false;
        }

        return (query[offset] | 32) == 'e'
                && (query[offset + 1] | 32) == 'x'
                && (query[offset + 2] | 32) == 'p'
                && (query[offset + 3] | 32) == 'l'
                && (query[offset + 4] | 32) == 'a'
                && (query[offset + 5] | 32) == 'i'
                && (query[offset + 6] | 32) == 'n'
                && isSpace(query[offset + 7]);
    }

    private static boolean parseSelectKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 's'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'c'
                && (query[offset + 5] | 32) == 't'
                && isSpace(query[offset + 6]);
    }

    private static boolean parseUpdateKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 'd'
                && (query[offset + 3] | 32) == 'a'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e'
                && isSpace(query[offset + 6]);
    }

    private static boolean parseUpsertKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'u'
                && (query[offset + 1] | 32) == 'p'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't'
                && isSpace(query[offset + 6]);
    }

    private static boolean parseInsertKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'i'
                && (query[offset + 1] | 32) == 'n'
                && (query[offset + 2] | 32) == 's'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 'r'
                && (query[offset + 5] | 32) == 't'
                && isSpace(query[offset + 6]);
    }

    private static boolean parseDeleteKeyword(char[] query, int offset) {
        if (query.length < (offset + 7)) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'l'
                && (query[offset + 3] | 32) == 'e'
                && (query[offset + 4] | 32) == 't'
                && (query[offset + 5] | 32) == 'e'
                && isSpace(query[offset + 6]);
    }

    private static boolean parseReplaceKeyword(char[] query, int offset) {
        if (query.length < (offset + 8)) {
            return false;
        }

        return (query[offset] | 32) == 'r'
                && (query[offset + 1] | 32) == 'e'
                && (query[offset + 2] | 32) == 'p'
                && (query[offset + 3] | 32) == 'l'
                && (query[offset + 4] | 32) == 'a'
                && (query[offset + 5] | 32) == 'c'
                && (query[offset + 8] | 32) == 'e'
                && isSpace(query[offset + 7]);
    }
}
