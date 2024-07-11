package tech.ydb.jdbc.query;

/**
 *
 * @author Aleksandr Gorshenin
 */
public enum QueryCmd {
    SELECT,
    CREATE_ALTER_DROP,
    INSERT_UPSERT,
    UPDATE_REPLACE_REMOVE
}
