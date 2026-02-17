package tech.ydb.jdbc.spi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.core.grpc.YdbHeaders;
import tech.ydb.jdbc.impl.helper.ExceptionAssert;
import tech.ydb.jdbc.impl.helper.JdbcUrlHelper;
import tech.ydb.proto.query.v1.QueryServiceGrpc;
import tech.ydb.proto.table.v1.TableServiceGrpc;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class WithTraceIdSpiTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static final JdbcUrlHelper jdbcURL = new JdbcUrlHelper(ydb)
            .withArg("cacheConnectionsInDriver", "false")
            .withArg("channelInitializer", WithTraceIdSpiTest.TraceIdCapturer.class.getName());

    private void executeQuery(Connection conn) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT 1 + 2")) {
            Assertions.assertTrue(rs.next());
            Assertions.assertFalse(rs.next());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    public void defaultUsageTest(String useQS) throws SQLException {
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new QuerySpiTestLoader(prev, WithTraceIdSpi.class));

        try (Connection conn = DriverManager.getConnection(jdbcURL.withArg("useQueryService", useQS).build())) {
            conn.setAutoCommit(false);

            WithTraceIdSpi.use("test-id");

            executeQuery(conn);
            Assertions.assertEquals("test-id-1", TraceIdCapturer.lastTraceID);
            executeQuery(conn);
            Assertions.assertEquals("test-id-2", TraceIdCapturer.lastTraceID);
            executeQuery(conn);
            Assertions.assertEquals("test-id-3", TraceIdCapturer.lastTraceID);

            conn.commit();

            executeQuery(conn);
            Assertions.assertFalse(TraceIdCapturer.lastTraceID.startsWith("test-id")); // no custom traces on a new transaction

            WithTraceIdSpi.use("test-id2");
            executeQuery(conn);
            Assertions.assertEquals("test-id2-1", TraceIdCapturer.lastTraceID);
            try (Statement st = conn.createStatement()) {
                // transaction was rollbacked
                ExceptionAssert.ydbException("code = GENERIC_ERROR", () -> st.executeQuery("SELECT 1 + 'test'u"));
            }
            Assertions.assertEquals("test-id2-2", TraceIdCapturer.lastTraceID);

            executeQuery(conn);
            Assertions.assertFalse(TraceIdCapturer.lastTraceID.startsWith("test-id")); // no custom traces on a new transaction

            conn.commit();
        } finally {
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    public static class TraceIdCapturer implements Consumer<ManagedChannelBuilder<?>>, ClientInterceptor {
        public static volatile String lastTraceID = null;

        @Override
        public void accept(ManagedChannelBuilder<?> builder) {
            builder.intercept(this);
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel channel) {
            if (method == QueryServiceGrpc.getExecuteQueryMethod() || method == TableServiceGrpc.getExecuteDataQueryMethod()) {
                return new ProxyClientCall<>(channel, method, callOptions);
            }
            return channel.newCall(method, callOptions);
        }

        private class ProxyClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
            private final ClientCall<ReqT, RespT> delegate;

            private ProxyClientCall(Channel channel, MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions) {
                this.delegate = channel.newCall(method, callOptions);
            }

            @Override
            public void request(int numMessages) {
                delegate.request(numMessages);
            }

            @Override
            public void cancel(@Nullable String message, @Nullable Throwable cause) {
                delegate.cancel(message, cause);
            }

            @Override
            public void halfClose() {
                delegate.halfClose();
            }

            @Override
            public void setMessageCompression(boolean enabled) {
                delegate.setMessageCompression(enabled);
            }

            @Override
            public boolean isReady() {
                return delegate.isReady();
            }

            @Override
            public Attributes getAttributes() {
                return delegate.getAttributes();
            }

            @Override
            public void start(ClientCall.Listener<RespT> listener, Metadata headers) {
                lastTraceID = headers.get(YdbHeaders.TRACE_ID);
                delegate.start(new ProxyListener(listener), headers);
            }

            @Override
            public void sendMessage(ReqT message) {
                delegate.sendMessage(message);
            }

            private class ProxyListener extends ClientCall.Listener<RespT> {
                private final ClientCall.Listener<RespT> delegate;

                public ProxyListener(ClientCall.Listener<RespT> delegate) {
                    this.delegate = delegate;
                }

                @Override
                public void onHeaders(Metadata headers) {
                    delegate.onHeaders(headers);
                }

                @Override
                public void onMessage(RespT message) {
                    delegate.onMessage(message);
                }

                @Override
                public void onClose(Status status, Metadata trailers) {
                    delegate.onClose(status, trailers);
                }

                @Override
                public void onReady() {
                    delegate.onReady();
                }
            }
        }
    }
}
