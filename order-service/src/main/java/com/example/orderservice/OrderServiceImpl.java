package com.example.orderservice;

import com.example.orders.v1.CreateOrderRequest;
import com.example.orders.v1.CreateOrderResponse;
import com.example.orders.v1.GetOrderRequest;
import com.example.orders.v1.Order;
import com.example.orders.v1.OrderItem;
import com.example.orders.v1.OrderServiceGrpc;
import com.example.orders.v1.OrderStatus;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OrderServiceImpl extends OrderServiceGrpc.OrderServiceImplBase {

    private final Map<String, Order> orders = new ConcurrentHashMap<>();

    @Override
    public void createOrder(CreateOrderRequest request,
                            StreamObserver<CreateOrderResponse> responseObserver) {
        if (request.getCustomerId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("customer_id is required").asRuntimeException());
            return;
        }
        if (request.getItemsCount() == 0) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("at least one item is required").asRuntimeException());
            return;
        }
        for (OrderItem item : request.getItemsList()) {
            if (item.getSku().isBlank() || item.getQuantity() <= 0) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("each item needs a sku and quantity > 0").asRuntimeException());
                return;
            }
        }

        Instant now = Instant.now();
        Order order = Order.newBuilder()
                .setOrderId(UUID.randomUUID().toString())
                .setCustomerId(request.getCustomerId())
                .addAllItems(request.getItemsList())
                .setStatus(OrderStatus.ORDER_STATUS_CREATED)
                .setCreatedAt(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .build();
        orders.put(order.getOrderId(), order);

        responseObserver.onNext(CreateOrderResponse.newBuilder()
                .setOrderId(order.getOrderId())
                .setStatus(order.getStatus())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getOrder(GetOrderRequest request, StreamObserver<Order> responseObserver) {
        Order order = orders.get(request.getOrderId());
        if (order == null) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("order " + request.getOrderId() + " not found")
                    .asRuntimeException());
            return;
        }
        responseObserver.onNext(order);
        responseObserver.onCompleted();
    }
}

