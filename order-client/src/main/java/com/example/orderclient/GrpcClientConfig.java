package com.example.orderclient;

import com.example.orders.v1.OrderServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GrpcClientConfig {

    @Bean(destroyMethod = "shutdown")
    public ManagedChannel channel(@Value("${grpc.client.host:localhost}") String host,
                                  @Value("${grpc.client.port:9090}") int port) {
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // sin TLS, solo para desarrollo
                .build();
    }

    @Bean
    public OrderServiceGrpc.OrderServiceBlockingStub blockingStub(ManagedChannel channel) {
        return OrderServiceGrpc.newBlockingStub(channel);
    }

    @Bean
    public OrderServiceGrpc.OrderServiceStub asyncStub(ManagedChannel channel) {
        return OrderServiceGrpc.newStub(channel);
    }
}
