package tech.ydb.jdbc.settings;

import java.util.Collection;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.BalancingSettings;
import tech.ydb.core.grpc.GrpcTransportBuilder;

public class YdbConnectionProperty<T> extends AbstractYdbProperty<T, GrpcTransportBuilder> {
    private static final PropertiesCollector<YdbConnectionProperty<?>> PROPERTIES = new PropertiesCollector<>();

    public static final YdbConnectionProperty<String> LOCAL_DATACENTER =
            new YdbConnectionProperty<>(
                    "localDatacenter",
                    "Local Datacenter",
                    null,
                    String.class,
                    PropertyConverter.stringValue(),
                    (builder, value) -> {
                        if (value != null && !value.isEmpty()) {
                            builder.withBalancingSettings(BalancingSettings.fromLocation(value));
                        }
                    });

    public static final YdbConnectionProperty<Boolean> SECURE_CONNECTION =
            new YdbConnectionProperty<>(
                    "secureConnection",
                    "Use TLS connection",
                    null,
                    Boolean.class,
                    PropertyConverter.booleanValue(),
                    (builder, value) -> {
                        if (value) {
                            builder.withSecureConnection();
                        }
                    });

    public static final YdbConnectionProperty<byte[]> SECURE_CONNECTION_CERTIFICATE =
            new YdbConnectionProperty<>(
                    "secureConnectionCertificate",
                    "Use TLS connection with certificate from provided path",
                    null,
                    byte[].class,
                    PropertyConverter.byteFileReference(),
                    GrpcTransportBuilder::withSecureConnection);

    public static final YdbConnectionProperty<AuthProvider> TOKEN =
            new YdbConnectionProperty<>(
                    "token",
                    "Token-based authentication",
                    null,
                    AuthProvider.class,
                    value -> new TokenAuthProvider(PropertyConverter.stringFileReference().convert(value)),
                    GrpcTransportBuilder::withAuthProvider);

    public static final YdbConnectionProperty<AuthProvider> SERVICE_ACCOUNT_FILE =
            new YdbConnectionProperty<>(
                    "saFile",
                    "Service account file based authentication",
                    null,
                    AuthProvider.class,
                    value -> CloudAuthHelper.getServiceAccountFileAuthProvider(
                            PropertyConverter.stringFileReference().convert(value)
                    ),
                    GrpcTransportBuilder::withAuthProvider);

    public static final YdbConnectionProperty<Boolean> USE_METADATA =
            new YdbConnectionProperty<>(
                    "useMetadata",
                    "Use metadata service for authentication",
                    null,
                    Boolean.class,
                    PropertyConverter.booleanValue(),
                    (builder, value) -> {
                        if (value) {
                            builder.withAuthProvider(CloudAuthHelper.getMetadataAuthProvider());
                        }
                    });

    private YdbConnectionProperty(String name,
                                    String description,
                                    @Nullable String defaultValue,
                                    Class<T> type,
                                    PropertyConverter<T> converter,
                                    BiConsumer<GrpcTransportBuilder, T> setter) {
        super(name, description, defaultValue, type, converter, setter);
        PROPERTIES.register(this);
    }

    public static Collection<YdbConnectionProperty<?>> properties() {
        return PROPERTIES.properties();
    }
}
