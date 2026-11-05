package com.example.orderservice;

import io.grpc.Metadata;

public final class MetadataKeys {

    public static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> CORRELATION_ID =
            Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

    private MetadataKeys() {
    }
}
