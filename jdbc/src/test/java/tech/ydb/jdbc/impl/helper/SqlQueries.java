package tech.ydb.jdbc.impl.helper;

import tech.ydb.jdbc.settings.YdbLookup;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SqlQueries {
    private static final String CREATE_TABLE = YdbLookup.stringFileReference("classpath:sql/create.sql");
    private static final String DROP_TABLE = YdbLookup.stringFileReference("classpath:sql/drop.sql");
    private static final String INIT_TABLE = YdbLookup.stringFileReference("classpath:sql/init.sql");

    private static final String SELECT = YdbLookup.stringFileReference("classpath:sql/select.sql");

    private static final String NAMED_UPSERT = YdbLookup.stringFileReference("classpath:sql/upsert/named.sql");

    private static final String SELECT_ALL = "select * from ${tableName}";
    private static final String DELETE_ALL = "delete from ${tableName}";

    private final String tableName;

    public SqlQueries(String tableName) {
        this.tableName = tableName;
    }

    public String withTableName(String query) {
        return query.replace("${tableName}", tableName);
    }

    public String createTableSQL() {
        return withTableName(CREATE_TABLE);
    }

    public String dropTableSQL() {
        return withTableName(DROP_TABLE);
    }

    public String initTableSQL() {
        return withTableName(INIT_TABLE);
    }

    /** @return select * from ${tableName} */
    public String selectAllSQL() {
        return withTableName(SELECT_ALL);
    }

    /** @return select key, c_Bool, c_Int8, ... , from ${tableName} */
    public String selectSQL() {
        return withTableName(SELECT);
    }

    /** @return delete from ${tableName} */
    public String deleteAllSQL() {
        return withTableName(DELETE_ALL);
    }

    private static String withTableName(String query, String tableName) {
        return query.replace("${tableName}", tableName);
    }

    public static String selectAllValuesSql(String tableName) {
        return withTableName(SELECT_ALL, tableName);
    }

    public static String upsertAllValuesSql(String tableName) {
        return withTableName(INIT_TABLE, tableName);
    }

    public static String namedUpsertSQL(String tableName) {
        return withTableName(NAMED_UPSERT, tableName);
    }
}
