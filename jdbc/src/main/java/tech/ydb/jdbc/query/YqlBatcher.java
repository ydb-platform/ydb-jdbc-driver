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
        INSERT
    }
    private enum State {
        INIT,
        CMD,
        INTO,
        TABLE_NAME,
        COLUMNS_OPEN_PAREN_OR_COMMA,
        COLUMN_NAME,
        COLUMNS_CLOSE_PAREN,
        VALUES,
        VALUES_OPEN_PAREN_OR_COMMA,
        COLUMN_VALUE,
        VALUES_CLOSE_PAREN,

        ERROR
    }

    private State state = State.INIT;
    private Cmd cmd = null;
    private String tableName = null;
    private final List<String> columns = new ArrayList<>();
    private final List<String> values = new ArrayList<>();

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

    public void clear() {
        this.state = State.INIT;
        this.cmd = null;
        this.tableName = null;
        this.columns.clear();
        this.values.clear();
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
            state = State.COLUMNS_OPEN_PAREN_OR_COMMA;
            return;
        }
        if (state == State.VALUES) {
            state = State.VALUES_OPEN_PAREN_OR_COMMA;
            return;
        }
        state = State.ERROR;
    }

    public void readCloseParen() {
        if (state == State.COLUMN_NAME) {
            state = State.COLUMNS_CLOSE_PAREN;
            return;
        }
        if (state == State.COLUMN_VALUE) {
            state = State.VALUES_CLOSE_PAREN;
            return;
        }
        state = State.ERROR;
    }

    public void readComma() {
        if (state == State.COLUMN_NAME) {
            state = State.COLUMNS_OPEN_PAREN_OR_COMMA;
            return;
        }
        if (state == State.COLUMN_VALUE) {
            state = State.VALUES_OPEN_PAREN_OR_COMMA;
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
        if (state == State.VALUES_OPEN_PAREN_OR_COMMA) {
            values.add("?");
            state = State.COLUMN_VALUE;
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

        if (state == State.COLUMNS_OPEN_PAREN_OR_COMMA) {
            columns.add(unquote(query, start, length));
            state = State.COLUMN_NAME;
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
