package tech.ydb.jdbc.exception;

import tech.ydb.core.Status;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbStatusable {
    Status getStatus();
}
