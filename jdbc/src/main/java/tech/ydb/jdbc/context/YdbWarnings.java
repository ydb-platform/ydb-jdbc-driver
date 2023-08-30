package tech.ydb.jdbc.context;

import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tech.ydb.core.Issue;
import tech.ydb.core.Status;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbWarnings {
    private final List<Issue> issues = new ArrayList<>();

    public void addStatusIssues(Status status) {
        issues.addAll(Arrays.asList(status.getIssues()));
    }

    public void clearWarnings() {
        this.issues.clear();
    }

    public SQLWarning toSQLWarnings() {
        SQLWarning firstWarning = null;
        SQLWarning warning = null;
        for (Issue issue : issues) {
            SQLWarning nextWarning = new SQLWarning(issue.toString(), null, issue.getCode());
            if (firstWarning == null) {
                firstWarning = nextWarning;
            }
            if (warning != null) {
                warning.setNextWarning(nextWarning);
            }
            warning = nextWarning;
        }
        return firstWarning;
    }
}
