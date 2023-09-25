package tech.ydb.jdbc.settings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public enum FakeTxMode {
    ERROR,
    FAKE_TX,
    SHADOW_COMMIT,
}
