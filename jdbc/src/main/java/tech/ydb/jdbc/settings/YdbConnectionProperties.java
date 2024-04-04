package tech.ydb.jdbc.settings;

import java.sql.SQLException;
import java.util.Properties;

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

    static final YdbProperty<String> IAM_ENDPOINT = YdbProperty.content("iamEndpoint",
            "Custom IAM endpoint for the service account authentication");

    static final YdbProperty<String> METADATA_URL = YdbProperty.content("metadataURL",
            "Custom URL for the metadata service authentication");

    private final String username;
    private final String password;

    private final YdbValue<String> localDatacenter;
    private final YdbValue<Boolean> useSecureConnection;
    private final YdbValue<byte[]> secureConnectionCertificate;
    private final YdbValue<String> token;
    private final YdbValue<String> serviceAccountFile;
    private final YdbValue<Boolean> useMetadata;
    private final YdbValue<String> iamEndpoint;
    private final YdbValue<String> metadataUrl;

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
        this.iamEndpoint = IAM_ENDPOINT.readValue(props);
        this.metadataUrl = METADATA_URL.readValue(props);
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
            String json = serviceAccountFile.getValue();
            if (iamEndpoint.hasValue()) {
                String endpoint = iamEndpoint.getValue();
                builder = builder.withAuthProvider(CloudAuthHelper.getServiceAccountJsonAuthProvider(json, endpoint));
            } else {
                builder = builder.withAuthProvider(CloudAuthHelper.getServiceAccountJsonAuthProvider(json));
            }
        }

        if (useMetadata.hasValue()) {
            if (metadataUrl.hasValue()) {
                String url = metadataUrl.getValue();
                builder = builder.withAuthProvider(CloudAuthHelper.getMetadataAuthProvider(url));
            } else {
                builder = builder.withAuthProvider(CloudAuthHelper.getMetadataAuthProvider());
            }
        }

        if (username != null && !username.isEmpty()) {
            builder = builder.withAuthProvider(new StaticCredentials(username, password));
        }

        return builder;
    }
}
