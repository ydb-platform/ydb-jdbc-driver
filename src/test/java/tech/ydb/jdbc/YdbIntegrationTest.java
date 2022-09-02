package tech.ydb.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbIntegrationTest {
    private final static Logger logger = LoggerFactory.getLogger(YdbIntegrationTest.class);

    public static final String SKIP_DOCKER_TESTS = "SKIP_DOCKER_TESTS";
    public static final String TRUE = "true";

    private static String endpoint;
    private static String database;
    private static YdbDockerContainer container;

    @BeforeAll
    public static void beforeAll() {
        String ydbDatabase = System.getenv("YDB_DATABASE");
        String ydbEndpoint = System.getenv("YDB_ENDPOINT");

        if (ydbEndpoint != null) {
            logger.info("use YDB receipt with endpoint {} and database {}", ydbEndpoint, ydbDatabase);
            container = null;
            endpoint = ydbEndpoint;
            database = ydbDatabase;
        } else {
            logger.info("set up YDB docker container");
            container = YdbDockerContainer.createAndStart();
            endpoint = container.nonSecureEndpoint();
            database = "/local";
        }
    }
    
    @AfterAll
    public static void shutdown() {
        if (container != null) {
            logger.info("stop YDB docker container");
            container.stop();
        }
    }

    public static String jdbcURl() {
        return String.format("jdbc:ydb:%s/%s", endpoint, database);
    }
}
