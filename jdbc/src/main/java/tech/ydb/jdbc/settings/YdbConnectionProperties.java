package tech.ydb.jdbc.settings;

import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.BalancingSettings;
import tech.ydb.core.grpc.GrpcCompression;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.jdbc.YdbDriver;


public class YdbConnectionProperties {
    private static final Logger LOGGER = Logger.getLogger(YdbDriver.class.getName());

    static final YdbProperty<String> TOKEN = YdbProperty.content(YdbConfig.TOKEN_KEY, "Authentication token");

    static final YdbProperty<String> TOKEN_FILE = YdbProperty.file("tokenFile",
            "Path to token file for the token-based authentication");

    static final YdbProperty<String> LOCAL_DATACENTER = YdbProperty.string("localDatacenter",
            "Local Datacenter");

    static final YdbProperty<Boolean> USE_SECURE_CONNECTION = YdbProperty.bool("secureConnection",
            "Use TLS connection");

    static final YdbProperty<byte[]> SECURE_CONNECTION_CERTIFICATE = YdbProperty.fileBytes(
            "secureConnectionCertificate", "Use TLS connection with certificate from provided path"
    );

    @Deprecated
    static final YdbProperty<String> SERVICE_ACCOUNT_FILE = YdbProperty.content("saFile",
            "Service account file based authentication");

    static final YdbProperty<String> SA_KEY_FILE = YdbProperty.file("saKeyFile",
            "Path to key file for the service account authentication");

    static final YdbProperty<Boolean> USE_METADATA = YdbProperty.bool("useMetadata",
            "Use metadata service for authentication");

    static final YdbProperty<String> IAM_ENDPOINT = YdbProperty.content("iamEndpoint",
            "Custom IAM endpoint for the service account authentication");

    static final YdbProperty<String> METADATA_URL = YdbProperty.content("metadataURL",
            "Custom URL for the metadata service authentication");

    static final YdbProperty<String> GRPC_COMPRESSION = YdbProperty.string(
            "grpcCompression", "Use specified GRPC compressor (supported only none and gzip)"
    );

    private final String username;
    private final String password;

    private final YdbValue<String> localDatacenter;
    private final YdbValue<Boolean> useSecureConnection;
    private final YdbValue<byte[]> secureConnectionCertificate;
    private final YdbValue<String> token;
    private final YdbValue<String> tokenFile;
    private final YdbValue<String> serviceAccountFile;
    private final YdbValue<String> saKeyFile;
    private final YdbValue<Boolean> useMetadata;
    private final YdbValue<String> iamEndpoint;
    private final YdbValue<String> metadataUrl;
    private final YdbValue<String> grpcCompression;

    public YdbConnectionProperties(YdbConfig config) throws SQLException {
        this.username = config.getUsername();
        this.password = config.getPassword();

        Properties props = config.getProperties();

        this.localDatacenter = LOCAL_DATACENTER.readValue(props);
        this.useSecureConnection = USE_SECURE_CONNECTION.readValue(props);
        this.secureConnectionCertificate = SECURE_CONNECTION_CERTIFICATE.readValue(props);
        this.token = TOKEN.readValue(props);
        this.tokenFile = TOKEN_FILE.readValue(props);
        this.serviceAccountFile = SERVICE_ACCOUNT_FILE.readValue(props);
        this.saKeyFile = SA_KEY_FILE.readValue(props);
        this.useMetadata = USE_METADATA.readValue(props);
        this.iamEndpoint = IAM_ENDPOINT.readValue(props);
        this.metadataUrl = METADATA_URL.readValue(props);
        this.grpcCompression = GRPC_COMPRESSION.readValue(props);
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

        String usedProvider = null;

        if (username != null && !username.isEmpty()) {
            builder = builder.withAuthProvider(new StaticCredentials(username, password));
            usedProvider = "username & password credentials";
        }

        if (useMetadata.hasValue()) {
            if (usedProvider != null) {
                LOGGER.log(Level.WARNING, "Dublicate authentication config! Metadata credentials replaces {0}",
                        usedProvider);
            }

            if (metadataUrl.hasValue()) {
                String url = metadataUrl.getValue();
                builder =  builder.withAuthProvider(CloudAuthHelper.getMetadataAuthProvider(url));
            } else {
                builder =  builder.withAuthProvider(CloudAuthHelper.getMetadataAuthProvider());
            }
            usedProvider = "metadata credentials";
        }

        if (tokenFile.hasValue()) {
            if (usedProvider != null) {
                LOGGER.log(Level.WARNING, "Dublicate authentication config! Token credentials replaces {0}",
                        usedProvider);
            }
            builder = builder.withAuthProvider(new TokenAuthProvider(tokenFile.getValue()));
            usedProvider = "token file credentitals";
        }

        if (token.hasValue()) {
            if (usedProvider != null) {
                LOGGER.log(Level.WARNING, "Dublicate authentication config! Token credentials replaces {0}",
                        usedProvider);
            }
            builder = builder.withAuthProvider(new TokenAuthProvider(token.getValue()));
            usedProvider = "token value credentitals";
        }

        if (saKeyFile.hasValue()) {
            if (usedProvider != null) {
                LOGGER.log(Level.WARNING, "Dublicate authentication config! Token credentials replaces {0}",
                        usedProvider);
            }
            String json = saKeyFile.getValue();
            if (iamEndpoint.hasValue()) {
                String endpoint = iamEndpoint.getValue();
                builder = builder.withAuthProvider(CloudAuthHelper.getServiceAccountJsonAuthProvider(json, endpoint));
            } else {
                builder = builder.withAuthProvider(CloudAuthHelper.getServiceAccountJsonAuthProvider(json));
            }
            usedProvider = "service account credentitals";
        }

        if (serviceAccountFile.hasValue()) {
            LOGGER.warning("Option 'saFile' is deprecated and will be removed in next versions. "
                    + "Use options 'saKeyFile' instead");
            if (usedProvider != null) {
                LOGGER.log(Level.WARNING, "Dublicate authentication config! Token credentials replaces {0}",
                        usedProvider);
            }
            String json = serviceAccountFile.getValue();
            if (iamEndpoint.hasValue()) {
                String endpoint = iamEndpoint.getValue();
                builder = builder.withAuthProvider(CloudAuthHelper.getServiceAccountJsonAuthProvider(json, endpoint));
            } else {
                builder = builder.withAuthProvider(CloudAuthHelper.getServiceAccountJsonAuthProvider(json));
            }
        }

        if (grpcCompression.hasValue()) {
            String value = grpcCompression.getValue();
            if ("none".equalsIgnoreCase(value)) {
                builder = builder.withGrpcCompression(GrpcCompression.NO_COMPRESSION);
            } else if ("gzip".equalsIgnoreCase(value)) {
                builder = builder.withGrpcCompression(GrpcCompression.GZIP);
            } else {
                LOGGER.log(Level.WARNING, "Unknown value for option 'grpcCompression' : {0}", value);
            }
        }

        return builder;
    }
}
