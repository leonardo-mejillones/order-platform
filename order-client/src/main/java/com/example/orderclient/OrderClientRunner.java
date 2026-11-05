package com.example.orderclient;

import com.example.orders.v1.CreateOrderRequest;
import com.example.orders.v1.CreateOrderResponse;
import com.example.orders.v1.GetOrderRequest;
import com.example.orders.v1.OrderEvent;
import com.example.orders.v1.OrderItem;
import com.example.orders.v1.OrderServiceGrpc;
import com.example.orders.v1.WatchOrderRequest;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class OrderClientRunner implements CommandLineRunner {

    private final OrderServiceGrpc.OrderServiceBlockingStub stub;
    private final ManagedChannel channel;

    public OrderClientRunner(OrderServiceGrpc.OrderServiceBlockingStub stub, ManagedChannel channel) {
        this.stub = stub;
        this.channel = channel;
    }

    @Override
    public void run(String... args) throws Exception {

        System.out.println("\n=== 1. Idempotencia: misma key, mismo payload ===");
        String key = UUID.randomUUID().toString();
        CreateOrderRequest req = newOrder("cust-001", "SKU-123", 2).setIdempotencyKey(key).build();
        CreateOrderResponse first = stub.createOrder(req);
        CreateOrderResponse second = stub.createOrder(req);
        System.out.println("first  = " + first.getOrderId());
        System.out.println("second = " + second.getOrderId());
        System.out.println("misma orden? " + first.getOrderId().equals(second.getOrderId()));

        System.out.println("\n=== 2. Idempotencia: misma key, payload distinto ===");
        expectError("reuse key", () ->
                stub.createOrder(newOrder("cust-001", "SKU-999", 5).setIdempotencyKey(key).build()));

        System.out.println("\n=== 3. Deadline (el servidor tarda 3 s, el cliente espera 500 ms) ===");
        expectError("slow call", () ->
                stub.withDeadlineAfter(500, TimeUnit.MILLISECONDS)
                        .createOrder(newOrder("slow-cust", "SKU-1", 1).build()));

        System.out.println("\n=== 4. Autenticación: stub SIN interceptor ===");
        OrderServiceGrpc.OrderServiceBlockingStub anonymous = OrderServiceGrpc.newBlockingStub(channel);
        expectError("no token", () ->
                anonymous.createOrder(newOrder("cust-001", "SKU-123", 1).build()));

        System.out.println("\n=== 5. Errores de negocio ===");
        expectError("get inexistente", () ->
                stub.getOrder(GetOrderRequest.newBuilder().setOrderId("does-not-exist").build()));
        expectError("create inválido", () ->
                stub.createOrder(CreateOrderRequest.newBuilder().setCustomerId("").build()));

        System.out.println("\n=== 6. WatchOrder: server streaming hasta estado terminal ===");
        CreateOrderResponse watched = stub.createOrder(newOrder("cust-watch", "SKU-1", 1).build());
        Iterator<OrderEvent> events = stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                .watchOrder(WatchOrderRequest.newBuilder().setOrderId(watched.getOrderId()).build());
        while (events.hasNext()) {
            OrderEvent event = events.next();
            System.out.println("evento: " + event.getStatus());
        }
        System.out.println("stream completado por el servidor");

        System.out.println("\n=== 7. WatchOrder: cancelación desde el cliente ===");
        CreateOrderResponse cancelled = stub.createOrder(newOrder("cust-cancel", "SKU-1", 1).build());
        Context.CancellableContext ctx = Context.current().withCancellation();
        try {
            ctx.run(() -> {
                Iterator<OrderEvent> it = stub.watchOrder(
                        WatchOrderRequest.newBuilder().setOrderId(cancelled.getOrderId()).build());
                System.out.println("primer evento: " + it.next().getStatus() + " -> cancelando el stream");
            });
        } finally {
            ctx.cancel(null);
        }
        Thread.sleep(500); // da tiempo a que el servidor loguee la cancelación
    }

    private static CreateOrderRequest.Builder newOrder(String customerId, String sku, int quantity) {
        return CreateOrderRequest.newBuilder()
                .setCustomerId(customerId)
                .addItems(OrderItem.newBuilder()
                        .setSku(sku).setQuantity(quantity).setUnitPriceCents(1999).build());
    }

    private static void expectError(String label, Runnable call) {
        try {
            call.run();
            System.out.println(label + " -> ¡no falló! (inesperado)");
        } catch (StatusRuntimeException e) {
            System.out.println(label + " -> " + e.getStatus().getCode()
                    + ": " + e.getStatus().getDescription());
        }
    }
}
