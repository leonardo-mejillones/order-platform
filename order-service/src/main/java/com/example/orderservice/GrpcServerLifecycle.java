package com.example.orderservice;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Component
public class GrpcServerLifecycle implements SmartLifecycle {

    private final OrderServiceImpl orderService;
    private final int port;
    private final String authToken;
    private Server server;
    private volatile boolean running;

    public GrpcServerLifecycle(OrderServiceImpl orderService,
                               @Value("${grpc.server.port:9090}") int port,
                               @Value("${grpc.server.auth-token:dev-token}") String authToken) {
        this.orderService = orderService;
        this.port = port;
        this.authToken = authToken;
    }

    @Override
    public void start() {
        try {
            // El ÚLTIMO interceptor de la lista es el PRIMERO en ejecutarse.
            // Logging va último para registrar también los rechazos de autenticación.
            server = ServerBuilder.forPort(port)
                    .addService(ServerInterceptors.intercept(
                            orderService,
                            new AuthServerInterceptor(authToken),
                            new LoggingServerInterceptor()))
                    .build()
                    .start();
        } catch (IOException e) {
            throw new IllegalStateException("Could not start gRPC server on port " + port, e);
        }
        running = true;
        System.out.println("gRPC server started on port " + port);

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
