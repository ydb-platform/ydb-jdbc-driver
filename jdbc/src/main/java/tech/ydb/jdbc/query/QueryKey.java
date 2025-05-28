package tech.ydb.jdbc.query;

import java.util.Objects;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class QueryKey {
    private final String query;
    private final String returning;

    public QueryKey(String query) {
        this.query = query;
        this.returning = null;
    }

    public QueryKey(String query, String[] columnNames) {
        this.query = query;
        this.returning = buildReturning(columnNames);
    }

    public String getQuery() {
        return query;
    }

    public String getReturning() {
        return returning;
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, returning);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryKey other = (QueryKey) obj;
        return Objects.equals(this.query, other.query) && Objects.equals(this.returning, other.returning);
    }

    private static String buildReturning(String[] columnNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("RETURNING ");
        if (columnNames.length == 1 && columnNames[0].charAt(0) == '*') {
            sb.append('*');
            return sb.toString();
        }

        for (int col = 0; col < columnNames.length; col++) {
            String columnName = columnNames[col];
            if (col > 0) {
                sb.append(", ");
            }

            if (columnName.charAt(0) == '`') {
                sb.append(columnName);
            } else {
                sb.append('`').append(columnName).append('`');
            }
        }

        return sb.toString();
    }
}
