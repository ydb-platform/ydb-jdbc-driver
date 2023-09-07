package tech.ydb.jdbc.query;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class JdbcLexer {
    /**
     * Parses JDBC query to replace all ? to YQL params.
     *
     * @param query jdbc query to parse
     * @param builder Ydb query builder
     */
    public static void buildQuery(String query, YdbQueryBuilder builder, boolean detectType) {
        int fragmentStart = 0;

        boolean nextExpression = true;

        char[] chars = query.toCharArray();

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
                    break;
                case '?':
                    builder.append(chars, fragmentStart, i - fragmentStart);
                    if (i + 1 < chars.length && chars[i + 1] == '?') /* replace ?? with ? */ {
                        builder.append('?');
                        i++; // make sure the coming ? is not treated as a bind
                    } else {
                        String binded = builder.createNextArgName();
                        builder.append(binded);
                    }
                    fragmentStart = i + 1;
                    break;

                default:
                    if (detectType && nextExpression && Character.isJavaIdentifierStart(ch)) {
                        nextExpression = false;

                        // Detect scheme expression
                        if (parseAlterKeyword(chars, i)
                                || parseCreateKeyword(chars, i)
                                || parseDropKeyword(chars, i)) {
                            builder.setQueryType(QueryType.SCHEME_QUERY);
                            break;
                        }

                        // Detect scan expression
                        if (parseScanKeyword(chars, i)) {
                            builder.setQueryType(QueryType.SCAN_QUERY);
                            builder.append(chars, fragmentStart, i - fragmentStart);
                            fragmentStart = i + 5; // Skip SCAN prefix
                            break;
                        }

                        // Detect explain expression
                        if (parseExplainKeyword(chars, i)) {
                            builder.setQueryType(QueryType.EXPLAIN_QUERY);
                            builder.append(chars, fragmentStart, i - fragmentStart);
                            fragmentStart = i + 8; // Skip EXPLAIN prefix
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

    public static boolean parseCreateKeyword(char[] query, int offset) {
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

    public static boolean parseDropKeyword(char[] query, int offset) {
        if (query.length < (offset + 5)) {
            return false;
        }

        return (query[offset] | 32) == 'd'
                && (query[offset + 1] | 32) == 'r'
                && (query[offset + 2] | 32) == 'o'
                && (query[offset + 3] | 32) == 'p'
                && isSpace(query[offset + 4]);
    }

    public static boolean parseScanKeyword(char[] query, int offset) {
        if (query.length < (offset + 5)) {
            return false;
        }

        return (query[offset] | 32) == 's'
                && (query[offset + 1] | 32) == 'c'
                && (query[offset + 2] | 32) == 'a'
                && (query[offset + 3] | 32) == 'n'
                && isSpace(query[offset + 4]);
    }

    public static boolean parseExplainKeyword(char[] query, int offset) {
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
}
