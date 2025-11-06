package tech.ydb.jdbc.query;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YqlListParser {

    private enum State {
        BEFORE_LIST,
        BEFORE_PARAM_OR_TUPLE,

        BEFORE_PARAM,
        BEFORE_TUPLE,
        BEFORE_TUPLE_PARAM,

        AFTER_TUPLE,
        AFTER_PARAM,
        AFTER_TUPLE_PARAM,

        AFTER_LIST,

        ERROR
    }

    private State state = State.BEFORE_LIST;

    private int listSize = 0;
    private int tupleSize = -1;

    private int currentTupleSize = 0;

    public boolean isNotStarted() {
        return state == State.BEFORE_LIST;
    }

    public boolean isCompleted() {
        return state == State.AFTER_LIST;
    }

    public int listSize() {
        return listSize;
    }

    public int tupleSize() {
        return tupleSize > 0 ? tupleSize : 1;
    }

    public boolean readOpenParen() {
        if (state == State.BEFORE_LIST) {
            state = State.BEFORE_PARAM_OR_TUPLE;
            return true;
        }

        if (state == State.BEFORE_PARAM_OR_TUPLE || state == State.BEFORE_TUPLE) {
            state = State.BEFORE_TUPLE_PARAM;
            return true;
        }

        state = State.ERROR;
        return false;
    }

    public boolean readCloseParen() {
        if (state == State.AFTER_TUPLE_PARAM) {
            if (tupleSize >= 0 && currentTupleSize != tupleSize) { // all tuples must have the same count of parameters
                state = State.ERROR;
                return false;
            }

            tupleSize = currentTupleSize;
            currentTupleSize = 0;
            listSize++;
            state = State.AFTER_TUPLE;
            return true;
        }

        if (state == State.AFTER_PARAM || state == State.AFTER_TUPLE) {
            state = State.AFTER_LIST;
            return true;
        }

        return false;
    }

    public boolean readComma() {
        if (state == State.AFTER_PARAM) {
            state = State.BEFORE_PARAM;
            return true;
        }
        if (state == State.AFTER_TUPLE_PARAM) {
            state = State.BEFORE_TUPLE_PARAM;
            return true;
        }
        if (state == State.AFTER_TUPLE) {
            state = State.BEFORE_TUPLE;
            return true;
        }

        state = State.ERROR;
        return false;
    }

    public boolean readParameter() {
        if (state == State.BEFORE_PARAM_OR_TUPLE || state == State.BEFORE_PARAM) {
            listSize++;
            state = State.AFTER_PARAM;
            return true;
        }
        if (state == State.BEFORE_TUPLE_PARAM) {
            currentTupleSize++;
            state = State.AFTER_TUPLE_PARAM;
            return true;
        }

        return false;
    }
}
