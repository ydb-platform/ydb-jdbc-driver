package tech.ydb.jdbc.query;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbExpression {
    static YdbExpression SELECT = new YdbExpression(false, true);
    static YdbExpression DDL = new YdbExpression(true, false);
    static YdbExpression OTHER_DML = new YdbExpression(false, false);

    private final boolean isDDL; // CREATE, DROP and ALTER
    private final boolean isSelect;

    private YdbExpression(boolean isDDL, boolean isSelect) {
        this.isDDL = isDDL;
        this.isSelect = isSelect;
    }

}
