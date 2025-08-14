package tech.ydb.jdbc.settings;

import java.util.function.Consumer;

import io.grpc.ManagedChannelBuilder;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class BadChannelInitilizer implements Consumer<ManagedChannelBuilder<?>> {
    private final String authority;

    public BadChannelInitilizer(String authority) {
        this.authority = authority;
    }

    @Override
    public void accept(ManagedChannelBuilder<?> t) {
        t.overrideAuthority(authority);
    }
}
