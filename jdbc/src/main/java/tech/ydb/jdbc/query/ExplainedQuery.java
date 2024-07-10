package tech.ydb.jdbc.query;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ExplainedQuery {
    private final String ast;
    private final String plan;

    public ExplainedQuery(String ast, String plan) {
        this.ast = ast;
        this.plan = plan;
    }

    public String getAst() {
        return this.ast;
    }

    public String getPlan() {
        return this.plan;
    }
}
