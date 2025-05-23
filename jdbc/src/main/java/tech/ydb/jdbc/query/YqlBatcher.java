package tech.ydb.jdbc.query;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlBatcher {
    public enum Cmd {
        UPSERT,
        INSERT,
        REPLACE,
        UPDATE,
        DELETE
    }
    private enum State {
        INIT,

        CMD,  // Readed init command, like UPDATE, UPSERT, INSERT, DELETE, REPLACE
        INTO, // Readed INTO keyword (only for INSERT/UPSERT/REPLACE)
        FROM, // Readed FROM keyword (only for DELETE)
        SET,        // Readed SET keyword (only for UPDATE)
        TABLE_NAME, // Readed table name

        COLUMNS_OPEN_PAREN,  // Readed '(' after table name, only for INSERT/UPSERT/REPLACE
        COLUMNS_COMMA,       // Readed ',' in column list, inside parens for INSERT/UPSERT/REPLACE, after SET for UPDATE
        COLUMNS_EQUAL,       // Readed '=' in column list, only after SET for UPDATE
        COLUMNS_NAME,        // Readed column name
        COLUMNS_VALUE,       // Readed column value (support only ?)
        COLUMNS_CLOSE_PAREN, // Readed ')', only for INSERT/UPSERT/REPLACE

        VALUES, // Readed VALUES keyword (only for INSERT/UPSERT/REPLACE)
        VALUES_OPEN_PAREN,   // Readed '(' after VALUES
        VALUES_COMMA,        // Readed ',' in values list, inside parens for VALUES
        VALUES_VALUE,        // Readed value (support only ?)
        VALUES_CLOSE_PAREN,  // Readed ')'

        WHERE, // Readed WHERE keyword (only for UPDATE/DELETE)
        WHERE_TABLE,  // Readed table name in WHERE clause as part of identifier
        WHERE_POINT,  // Readed '.' after table name in WHERE clause
        WHERE_COLUMN, // Readed column name in WHERE clause
        WHERE_EQUAL,  // Readed '=' in WHERE clause
        WHERE_VALUE,  // Readed column value in WHERE clause (support only ?)
        WHERE_AND,    // Readed AND keyword in WHERE clause

        SEMICOLON, // Readed ';' after whole expression
        ERROR
    }

    private State state = State.INIT;
    private Cmd cmd = null;
    private String tableName = null;
    private final List<String> columns = new ArrayList<>();
    private final List<String> values = new ArrayList<>();
    private final List<String> keyColumns = new ArrayList<>();
    private final List<String> keyValues = new ArrayList<>();

    public void setForcedUpsert() {
        cmd = Cmd.UPSERT;
    }

    public Cmd getCommand() {
        return cmd;
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

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public List<String> getKeyValues() {
        return keyValues;
    }

    public boolean isValidBatch() {
        if (cmd == null || tableName == null || tableName.isEmpty()) {
            return false;
        }
        if (cmd == Cmd.DELETE ^ columns.isEmpty()) {
            return false;
        }

        switch (cmd) {
            case INSERT:
            case UPSERT:
            case REPLACE:
                return (state == State.VALUES_CLOSE_PAREN || state == State.SEMICOLON)
                        && keyColumns.isEmpty()
                        && keyValues.isEmpty()
                        && columns.size() == values.size();
            case DELETE:
            case UPDATE:
                return (state == State.WHERE_VALUE || state == State.SEMICOLON)
                        && !keyColumns.isEmpty()
                        && columns.size() == values.size()
                        && keyColumns.size() == keyValues.size();
            default:
                return false;
        }
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

    public void readReplace() {
        if (state == State.INIT) {
            state = State.CMD;
            cmd = Cmd.REPLACE;
            return;
        }
        state = State.ERROR;
    }

    public void readUpdate() {
        if (state == State.INIT) {
            state = State.CMD;
            cmd = Cmd.UPDATE;
            return;
        }
        state = State.ERROR;
    }

    public void readDelete() {
        if (state == State.INIT) {
            state = State.CMD;
            cmd = Cmd.DELETE;
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
        if (state == State.COLUMNS_VALUE && cmd == Cmd.UPDATE) {
            state = State.COLUMNS_COMMA;
            return;
        }
        if (state == State.COLUMNS_NAME && (cmd == Cmd.INSERT || cmd == Cmd.UPSERT || cmd == Cmd.REPLACE)) {
            state = State.COLUMNS_COMMA;
            return;
        }
        if (state == State.VALUES_VALUE) {
            state = State.VALUES_COMMA;
            return;
        }
        state = State.ERROR;
    }

    public void readPoint() {
        if (state == State.WHERE_TABLE) {
            state = State.WHERE_POINT;
            return;
        }
        state = State.ERROR;
    }

    public void readEqual() {
        if (state == State.COLUMNS_NAME && cmd == Cmd.UPDATE) {
            state = State.COLUMNS_EQUAL;
            return;
        }
        if (state == State.WHERE_TABLE) { // special case with column name == table name
            keyColumns.add(tableName);
            state = State.WHERE_EQUAL;
            return;
        }
        if (state == State.WHERE_COLUMN && (cmd == Cmd.UPDATE || cmd == Cmd.DELETE)) {
            state = State.WHERE_EQUAL;
            return;
        }

        state = State.ERROR;
    }

    public void readSemiColon() {
        if (state == State.INIT || state == State.SEMICOLON) {
            return;
        }

        if (state == State.VALUES_CLOSE_PAREN || state == State.WHERE_VALUE) {
            state = State.SEMICOLON;
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
        if (cmd == Cmd.UPDATE && state == State.COLUMNS_EQUAL) {
            values.add("?");
            state = State.COLUMNS_VALUE;
            return;
        }

        if (state == State.WHERE_EQUAL) {
            keyValues.add("?");
            state = State.WHERE_VALUE;
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
            if (cmd == Cmd.UPDATE) {
                tableName = unquote(query, start, length);
                state = State.TABLE_NAME;
                return;
            }

            if (cmd == Cmd.DELETE) {
                if (length == 4
                        && (query[start] | 32) == 'f'
                        && (query[start + 1] | 32) == 'r'
                        && (query[start + 2] | 32) == 'o'
                        && (query[start + 3] | 32) == 'm') {
                    state = State.FROM;
                    return;
                }
            }

            if  (cmd == Cmd.INSERT || cmd == Cmd.REPLACE || cmd == Cmd.UPSERT) {
                if (length == 4
                        && (query[start] | 32) == 'i'
                        && (query[start + 1] | 32) == 'n'
                        && (query[start + 2] | 32) == 't'
                        && (query[start + 3] | 32) == 'o') {
                    state = State.INTO;
                    return;
                }
            }
        }

        if (state == State.TABLE_NAME && cmd == Cmd.UPDATE) {
            if (length == 3
                    && (query[start] | 32) == 's'
                    && (query[start + 1] | 32) == 'e'
                    && (query[start + 2] | 32) == 't') {
                state = State.SET;
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

        if (state == State.COLUMNS_VALUE || (state == State.TABLE_NAME && cmd == Cmd.DELETE)) {
            if (length == 5
                    && (query[start] | 32) == 'w'
                    && (query[start + 1] | 32) == 'h'
                    && (query[start + 2] | 32) == 'e'
                    && (query[start + 3] | 32) == 'r'
                    && (query[start + 4] | 32) == 'e') {
                state = State.WHERE;
                return;
            }
        }

        if (state == State.WHERE_VALUE) {
            if (length == 3
                    && (query[start] | 32) == 'a'
                    && (query[start + 1] | 32) == 'n'
                    && (query[start + 2] | 32) == 'd') {
                state = State.WHERE_AND;
                return;
            }
        }

        if (state == State.INTO && (cmd == Cmd.INSERT || cmd == Cmd.UPSERT || cmd == Cmd.REPLACE)) {
            tableName = unquote(query, start, length);
            state = State.TABLE_NAME;
            return;
        }

        if (state == State.FROM && cmd == Cmd.DELETE) {
            tableName = unquote(query, start, length);
            state = State.TABLE_NAME;
            return;
        }

        if (state == State.COLUMNS_OPEN_PAREN || state == State.COLUMNS_COMMA || state == State.SET) {
            columns.add(unquote(query, start, length));
            state = State.COLUMNS_NAME;
            return;
        }

        if (state == State.WHERE || state == State.WHERE_AND) {
            String identifier = unquote(query, start, length);
            if (tableName.equals(identifier)) {
                state = State.WHERE_TABLE;
            } else {
                keyColumns.add(identifier);
                state = State.WHERE_COLUMN;
            }
            return;
        }

        if (state == State.WHERE_POINT) {
            keyColumns.add(unquote(query, start, length));
            state = State.WHERE_COLUMN;
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
