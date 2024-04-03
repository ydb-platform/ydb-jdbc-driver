package tech.ydb.jdbc.settings;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.BalancingSettings;
import tech.ydb.core.grpc.GrpcTransportBuilder;


public class YdbConnectionProperties {
    static final YdbProperty<String> TOKEN = YdbProperty.content(YdbConfig.TOKEN_KEY, "Authentication token");

    static final YdbProperty<String> LOCAL_DATACENTER = YdbProperty.string("localDatacenter",
            "Local Datacenter");

    static final YdbProperty<Boolean> USE_SECURE_CONNECTION = YdbProperty.bool("secureConnection",
            "Use TLS connection");

    static final YdbProperty<byte[]> SECURE_CONNECTION_CERTIFICATE = YdbProperty.bytes("secureConnectionCertificate",
            "Use TLS connection with certificate from provided path");

    static final YdbProperty<String> SERVICE_ACCOUNT_FILE = YdbProperty.content("saFile",
            "Service account file based authentication");

    static final YdbProperty<Boolean> USE_METADATA = YdbProperty.bool("useMetadata",
            "Use metadata service for authentication");

    private final String username;
    private final String password;

    private final YdbPropertyValue<String> localDatacenter;
    private final YdbPropertyValue<Boolean> useSecureConnection;
    private final YdbPropertyValue<byte[]> secureConnectionCertificate;
    private final YdbPropertyValue<String> token;
    private final YdbPropertyValue<String> serviceAccountFile;
    private final YdbPropertyValue<Boolean> useMetadata;

    public YdbConnectionProperties(YdbConfig config) throws SQLException {
        this.username = config.getUsername();
        this.password = config.getPassword();

        Properties props = config.getProperties();

        this.localDatacenter = LOCAL_DATACENTER.readValue(props);
        this.useSecureConnection = USE_SECURE_CONNECTION.readValue(props);
        this.secureConnectionCertificate = SECURE_CONNECTION_CERTIFICATE.readValue(props);
        this.token = TOKEN.readValue(props);
        this.serviceAccountFile = SERVICE_ACCOUNT_FILE.readValue(props);
        this.useMetadata = USE_METADATA.readValue(props);
    }

    String getLocalDataCenter() {
        return localDatacenter.getValue();
    }

    String getToken() {
        return token.getValue();
    }

    byte[] getSecureConnectionCert() {
        return secureConnectionCertificate.getValue();
    }

    public GrpcTransportBuilder applyToGrpcTransport(GrpcTransportBuilder builder) {
        if (localDatacenter.hasValue()) {
            builder = builder.withBalancingSettings(BalancingSettings.fromLocation(localDatacenter.getValue()));
        }

        if (useSecureConnection.hasValue() && useSecureConnection.getValue()) {
            builder = builder.withSecureConnection();
        }

        if (secureConnectionCertificate.hasValue()) {
            builder = builder.withSecureConnection(secureConnectionCertificate.getValue());
        }

        if (token.hasValue()) {
            builder = builder.withAuthProvider(new TokenAuthProvider(token.getValue()));
        }

        if (serviceAccountFile.hasValue()) {
            builder = builder.withAuthProvider(
                    CloudAuthHelper.getServiceAccountJsonAuthProvider(serviceAccountFile.getValue())
            );
        }

        if (useMetadata.hasValue()) {
            builder = builder.withAuthProvider(CloudAuthHelper.getMetadataAuthProvider());
        }

        if (username != null && !username.isEmpty()) {
            builder = builder.withAuthProvider(new StaticCredentials(username, password));
        }

        // Use custom single thread scheduler because JDBC driver doesn't need to execute retries except for DISCOERY
        builder.withSchedulerFactory(() -> {
            final String namePrefix = "ydb-jdbc-scheduler[" + this.hashCode() +"]-thread-";
            final AtomicInteger threadNumber = new AtomicInteger(1);
            return Executors.newScheduledThreadPool(1, (Runnable r) -> {
                Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            });
        });

        return builder;
    }
}
