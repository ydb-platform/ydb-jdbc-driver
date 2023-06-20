package tech.ydb.jdbc.impl.helper;

import tech.ydb.jdbc.settings.YdbLookup;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class TestResources {
    private static final String CREATE_TABLE = YdbLookup.stringFileReference("classpath:sql/create_table.sql");
    private static final String SELECT_ALL_VALUES = YdbLookup.stringFileReference("classpath:sql/select_all_values.sql");
    private static final String UPSERT_ALL_VALUES = YdbLookup.stringFileReference("classpath:sql/upsert_all_values.sql");

    private TestResources() { }

    private static String withTableName(String query, String tableName) {
        return query.replace("${tableName}", tableName);
    }


    public static String createTableSql(String tableName) {
        return withTableName(CREATE_TABLE, tableName);
    }

    public static String selectAllValuesSql(String tableName) {
        return withTableName(SELECT_ALL_VALUES, tableName);
    }

    public static String upsertAllValuesSql(String tableName) {
        return withTableName(UPSERT_ALL_VALUES, tableName);
    }
}
