package tech.ydb.jdbc.query;

/**
 *
 * @author Aleksandr Gorshenin
 */
public enum QueryCmd {
    UNKNOWN,
    SELECT,
    /** CREATE, DROP, ALTER, GRANT, REVOKE */
    DDL,
    /** INSERT, UPSERT, UPDATE, REPLACE, DELETE */
    DML,
    /** BATCH UPDATE/BATCH DELETE */
    BATCH,
}
