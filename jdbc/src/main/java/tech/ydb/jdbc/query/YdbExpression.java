package tech.ydb.jdbc.query;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbExpression {
    static final YdbExpression SELECT = new YdbExpression(false, true);
    static final YdbExpression DDL = new YdbExpression(true, false);
    static final YdbExpression OTHER_DML = new YdbExpression(false, false);

    private final boolean isDDL; // CREATE, DROP and ALTER
    private final boolean isSelect;

    private YdbExpression(boolean isDDL, boolean isSelect) {
        this.isDDL = isDDL;
        this.isSelect = isSelect;
    }

    public boolean isDDL() {
        return isDDL;
    }

    public boolean isSelect() {
        return isSelect;
    }
}
