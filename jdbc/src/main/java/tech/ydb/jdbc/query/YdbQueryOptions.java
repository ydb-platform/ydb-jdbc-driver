package tech.ydb.jdbc.query;

import tech.ydb.jdbc.settings.YdbOperationProperties;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbQueryOptions {
    private final boolean isDetectQueryType;
    private final boolean isDetectJdbcParameters;
    private final boolean isDeclareJdbcParameters;
    private final boolean isEnforceSyntaxV1;

    public YdbQueryOptions(YdbOperationProperties props) {
        this.isDetectQueryType = props.isDetectSqlOperations();
        this.isDetectJdbcParameters = !props.isJdbcParametersSupportDisabled();
        this.isDeclareJdbcParameters = true;
        this.isEnforceSyntaxV1 = props.isEnforceSqlV1();
    }

    public boolean isEnforceSyntaxV1() {
        return isEnforceSyntaxV1;
    }

    public boolean isDetectQueryType() {
        return isDetectQueryType;
    }

    public boolean isDetectJdbcParameters() {
        return isDetectJdbcParameters;
    }

    public boolean isDeclareJdbcParameters() {
        return isDeclareJdbcParameters;
    }
}
