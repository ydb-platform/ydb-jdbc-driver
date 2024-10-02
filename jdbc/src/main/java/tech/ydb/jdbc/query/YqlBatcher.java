package tech.ydb.jdbc.query;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlBatcher {
    private enum Cmd {
        UPSERT,
        INSERT,
        UPDATE,
        DELETE
    }
    private enum State {
        INIT,

        CMD,  // Readed init command, like UPDATE, UPSERT, INSERT, DELETE
        INTO, // Readed INTO keyword (only for INSERT/UPSERT)
        TABLE_NAME, // Readed table name
        SET,        // Readed SET keyword (only for UPDATE)

        COLUMNS_OPEN_PAREN,  // Readed '(' after table name, only for INSERT/UPSERT
        COLUMNS_COMMA,       // Readed ',' in column list, inside parens for INSERT/UPSERT, after SET for UPDATE)
        COLUMNS_EQUAL,       // Readed '=' in column list, only after SET for UPDATE
        COLUMNS_NAME,        // Readed column name
        COLUMNS_VALUE,       // Readed column value (support only ?)
        COLUMNS_CLOSE_PAREN, // Readed ')', only for INSERT/UPSERT

        VALUES, // Readed VALUES keyword (only for INSERT/UPSERT)
        VALUES_OPEN_PAREN,   // Readed '(' after VALUES, only for INSERT/UPSERT
        VALUES_COMMA,        // Readed ',' in values list, inside parens for VALUES
        VALUES_VALUE,        // Readed value (support only ?)
        VALUES_CLOSE_PAREN,  // Readed ')', only for INSERT/UPSERT

        WHERE, // Readed WHERE keyword (only for UPDATE/DETELE)
        WHERE_COLUMN, // Readed column name in WHERE clause
        WHERE_EQUAL,  // Readed '=' in WHERE clause
        WHERE_VALUE,  // Readed column value in WHERE clause (support only ?)
        WHERE_AND,    // Readed AND keyword in WHERE clause

        ERROR
    }

    private State state = State.INIT;
    private Cmd cmd = null;
    private String tableName = null;
    private final List<String> columns = new ArrayList<>();
    private final List<String> keyColumns = new ArrayList<>();
    private final List<String> values = new ArrayList<>();

    public void setForcedUpsert() {
        cmd = Cmd.UPSERT;
    }

    public boolean isInsert() {
        return cmd == Cmd.INSERT;
    }

    public boolean isUpsert() {
        return cmd == Cmd.UPSERT;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getValues() {
        return values;
    }

    public boolean isValidBatch() {
        return state == State.VALUES_CLOSE_PAREN && cmd != null
                && tableName != null && !tableName.isEmpty()
                && !columns.isEmpty() && columns.size() == values.size();
    }

    public void readInsert() {
        if (state == State.INIT) {
            state = State.CMD;
            cmd = Cmd.INSERT;
            return;
        }
        state = State.ERROR;
    }

    public void readUpsert() {
        if (state == State.INIT) {
            state = State.CMD;
            cmd = Cmd.UPSERT;
            return;
        }
        state = State.ERROR;
    }

    public void readOpenParen() {
        if (state == State.TABLE_NAME) {
            state = State.COLUMNS_OPEN_PAREN;
            return;
        }
        if (state == State.VALUES) {
            state = State.VALUES_OPEN_PAREN;
            return;
        }
        state = State.ERROR;
    }

    public void readCloseParen() {
        if (state == State.COLUMNS_NAME) {
            state = State.COLUMNS_CLOSE_PAREN;
            return;
        }
        if (state == State.VALUES_VALUE) {
            state = State.VALUES_CLOSE_PAREN;
            return;
        }
        state = State.ERROR;
    }

    public void readComma() {
        if (state == State.COLUMNS_NAME) {
            state = State.COLUMNS_COMMA;
            return;
        }
        if (state == State.VALUES_VALUE) {
            state = State.VALUES_COMMA;
            return;
        }
        state = State.ERROR;
    }

    public void readSemiColon() {
        if (state == State.INIT || state == State.VALUES_CLOSE_PAREN) {
            return;
        }
        state = State.ERROR;
    }

    public void readParameter() {
        if (state == State.VALUES_OPEN_PAREN || state == State.VALUES_COMMA) {
            values.add("?");
            state = State.VALUES_VALUE;
            return;
        }
        state = State.ERROR;
    }

    public void readSingleQuoteLiteral(char[] query, int start, int length) {
        // NOT SUPPORTED YET
        state = State.ERROR;
    }

    public void readDoubleQuoteLiteral(char[] query, int start, int length) {
        // NOT SUPPORTED YET
        state = State.ERROR;
    }

    public void readIdentifier(char[] query, int start, int length) {
        if (state == State.CMD) {
            if (length == 4
                    && (query[start] | 32) == 'i'
                    && (query[start + 1] | 32) == 'n'
                    && (query[start + 2] | 32) == 't'
                    && (query[start + 3] | 32) == 'o') {
                state = State.INTO;
                return;
            }
        }
        if (state == State.COLUMNS_CLOSE_PAREN) {
            if (length == 6
                    && (query[start] | 32) == 'v'
                    && (query[start + 1] | 32) == 'a'
                    && (query[start + 2] | 32) == 'l'
                    && (query[start + 3] | 32) == 'u'
                    && (query[start + 4] | 32) == 'e'
                    && (query[start + 5] | 32) == 's') {
                state = State.VALUES;
                return;
            }
        }

        if (state == State.INTO) {
            tableName = unquote(query, start, length);
            state = State.TABLE_NAME;
            return;
        }

        if (state == State.COLUMNS_OPEN_PAREN || state == State.COLUMNS_COMMA) {
            columns.add(unquote(query, start, length));
            state = State.COLUMNS_NAME;
            return;
        }

        state = State.ERROR;
    }

    private String unquote(char[] chars, int start, int length) {
        if (chars[start] == '`' && chars[start + length - 1] == '`') {
            return String.valueOf(chars, start + 1, length - 2);
        }
        return String.valueOf(chars, start, length);
    }
}
