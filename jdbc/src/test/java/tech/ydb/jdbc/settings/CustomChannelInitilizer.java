package tech.ydb.jdbc.settings;

import java.util.function.Consumer;

import io.grpc.ManagedChannelBuilder;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class CustomChannelInitilizer implements Consumer<ManagedChannelBuilder<?>> {
    @Override
    public void accept(ManagedChannelBuilder<?> t) {
        t.usePlaintext();
    }
}
