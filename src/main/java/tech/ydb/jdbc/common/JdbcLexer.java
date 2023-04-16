package tech.ydb.jdbc.common;

import java.util.function.Supplier;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class JdbcLexer {

    /**
     * Parses JDBC query to replace all ? to YQL params.
     *
     * @param query jdbc query to parse
     * @param argGenerator generator for ? symbols
     * @return list of native queries
     */
    public static String jdbc2yql(String query, Supplier<String> argGenerator) {
        int fragmentStart = 0;

        char[] chars = query.toCharArray();

        StringBuilder yqlQuery = new StringBuilder(query.length() + 10);

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

                case '?':
                    yqlQuery.append(chars, fragmentStart, i - fragmentStart);
                    if (i + 1 < chars.length && chars[i + 1] == '?') /* replace ?? with ? */ {
                        yqlQuery.append('?');
                        i++; // make sure the coming ? is not treated as a bind
                    } else {
                        String binded = argGenerator.get();
                        yqlQuery.append(binded);
                    }
                    fragmentStart = i + 1;
                    break;

                default:
                    break;
            }
        }

        if (fragmentStart < chars.length) {
            yqlQuery.append(chars, fragmentStart, chars.length - fragmentStart);
        }

        return yqlQuery.toString();
    }

    public static int parseSingleQuotes(final char[] query, int offset) {
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

    public static int parseDoubleQuotes(final char[] query, int offset) {
        while (++offset < query.length && query[offset] != '"') {
            // do nothing
        }
        return offset;
    }

    public static int parseLineComment(final char[] query, int offset) {
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

    public static int parseBlockComment(final char[] query, int offset) {
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
}
