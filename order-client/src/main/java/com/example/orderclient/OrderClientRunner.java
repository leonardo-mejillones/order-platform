package com.example.orderclient;

import com.example.orders.v1.CreateOrderRequest;
import com.example.orders.v1.CreateOrderResponse;
import com.example.orders.v1.GetOrderRequest;
import com.example.orders.v1.Order;
import com.example.orders.v1.OrderItem;
import com.example.orders.v1.OrderServiceGrpc;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
public class OrderClientRunner implements CommandLineRunner {

    private final OrderServiceGrpc.OrderServiceBlockingStub blockingStub;
    private final OrderServiceGrpc.OrderServiceStub asyncStub;

    public OrderClientRunner(OrderServiceGrpc.OrderServiceBlockingStub blockingStub,
                             OrderServiceGrpc.OrderServiceStub asyncStub) {
        this.blockingStub = blockingStub;
        this.asyncStub = asyncStub;
    }

    @Override
    public void run(String... args) throws Exception {
        CreateOrderRequest request = CreateOrderRequest.newBuilder()
                .setCustomerId("cust-001")
                .addItems(OrderItem.newBuilder()
                        .setSku("SKU-123").setQuantity(2).setUnitPriceCents(1999).build())
                .build();

        // 1. Blocking stub con deadline
        System.out.println("=== 1. CreateOrder (blocking) ===");
        CreateOrderResponse created = blockingStub
                .withDeadlineAfter(2, TimeUnit.SECONDS)
                .createOrder(request);
        System.out.println("Created: " + created.getOrderId() + " " + created.getStatus());

        // 2. GetOrder
        System.out.println("=== 2. GetOrder (blocking) ===");
        Order order = blockingStub.getOrder(
                GetOrderRequest.newBuilder().setOrderId(created.getOrderId()).build());
        System.out.println("Fetched: " + order);

        // 3. Error esperado: NOT_FOUND
        System.out.println("=== 3. GetOrder inexistente ===");
        try {
            blockingStub.getOrder(GetOrderRequest.newBuilder().setOrderId("does-not-exist").build());
        } catch (StatusRuntimeException e) {
            System.out.println("Error esperado: " + e.getStatus().getCode()
                    + " - " + e.getStatus().getDescription());
        }

        // 4. Error esperado: INVALID_ARGUMENT
        System.out.println("=== 4. CreateOrder inválido ===");
        try {
            blockingStub.createOrder(CreateOrderRequest.newBuilder().setCustomerId("").build());
        } catch (StatusRuntimeException e) {
            System.out.println("Error esperado: " + e.getStatus().getCode()
                    + " - " + e.getStatus().getDescription());
        }

        // 5. Async stub con StreamObserver
        System.out.println("=== 5. CreateOrder (async) ===");
        CountDownLatch latch = new CountDownLatch(1);
        asyncStub.createOrder(request, new StreamObserver<>() {
            @Override
            public void onNext(CreateOrderResponse value) {
                System.out.println("Async created: " + value.getOrderId());
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Async error: " + t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("Async completed");
                latch.countDown();
            }
        });
        if (!latch.await(5, TimeUnit.SECONDS)) {
            System.err.println("Async call timed out");
        }
    }
}
