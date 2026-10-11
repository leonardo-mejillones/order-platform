package com.example.orderservice;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Component
public class GrpcServerLifecycle implements SmartLifecycle {

    private final OrderServiceImpl orderService;
    private final int port;
    private Server server;
    private volatile boolean running;

    public GrpcServerLifecycle(OrderServiceImpl orderService,
                               @Value("${grpc.server.port:9090}") int port) {
        this.orderService = orderService;
        this.port = port;
    }

    @Override
    public void start() {
        try {
            server = ServerBuilder.forPort(port)
                    .addService(orderService)
                    .build()
                    .start();
        } catch (IOException e) {
            throw new IllegalStateException("Could not start gRPC server on port " + port, e);
        }
        running = true;
        System.out.println("gRPC server started on port " + port);

        // Hilo no-daemon para que la JVM no termine (esta app no tiene servidor web)
        Thread await = new Thread(() -> {
            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "grpc-await");
        await.setDaemon(false);
        await.start();
    }

    @Override
    public void stop() {
        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                server.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
