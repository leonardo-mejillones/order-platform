package com.example.orderservice;

import com.example.orders.v1.CreateOrderRequest;
import com.example.orders.v1.CreateOrderResponse;
import com.example.orders.v1.GetOrderRequest;
import com.example.orders.v1.Order;
import com.example.orders.v1.OrderEvent;
import com.example.orders.v1.OrderItem;
import com.example.orders.v1.OrderServiceGrpc;
import com.example.orders.v1.OrderStatus;
import com.example.orders.v1.WatchOrderRequest;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class OrderServiceImpl extends OrderServiceGrpc.OrderServiceImplBase implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceImpl.class);

    private final Map<String, Order> orders = new ConcurrentHashMap<>();
    private final Map<String, String> idempotencyIndex = new ConcurrentHashMap<>(); // key -> orderId
    private final Map<String, List<ServerCallStreamObserver<OrderEvent>>> watchers = new ConcurrentHashMap<>();

    // Serializa "registrar watcher" y "cambiar estado + notificar" para no perder eventos
    // y para no llamar onNext concurrentemente sobre el mismo observer.
    private final Object lock = new Object();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "order-lifecycle");
        t.setDaemon(true);
        return t;
    });

    // ---------------------------------------------------------------- CreateOrder

    @Override
    public void createOrder(CreateOrderRequest request,
                            StreamObserver<CreateOrderResponse> responseObserver) {
        String validationError = validate(request);
        if (validationError != null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(validationError).asRuntimeException());
            return;
        }

        // Simulación de trabajo lento para practicar deadlines
        if (request.getCustomerId().startsWith("slow-") && !simulateSlowWork()) {
            return; // cancelado: nadie espera la respuesta
        }

        Order order;
        if (request.getIdempotencyKey().isBlank()) {
            order = storeNewOrder(request);
        } else {
            boolean[] createdNow = {false};
            // computeIfAbsent es atómico por key: dos requests concurrentes con la misma key
            // no pueden crear dos órdenes.
            String orderId = idempotencyIndex.computeIfAbsent(request.getIdempotencyKey(), k -> {
                createdNow[0] = true;
                return storeNewOrder(request).getOrderId();
            });
            order = orders.get(orderId);

            if (!createdNow[0] && !sameContent(order, request)) {
                responseObserver.onError(Status.ALREADY_EXISTS
                        .withDescription("idempotency_key already used with a different payload")
                        .asRuntimeException());
                return;
            }
        }

        responseObserver.onNext(CreateOrderResponse.newBuilder()
                .setOrderId(order.getOrderId())
                .setStatus(order.getStatus())
                .build());
        responseObserver.onCompleted();
    }

    private String validate(CreateOrderRequest request) {
        if (request.getCustomerId().isBlank()) {
            return "customer_id is required";
        }
        if (request.getItemsCount() == 0) {
            return "at least one item is required";
        }
        for (OrderItem item : request.getItemsList()) {
            if (item.getSku().isBlank() || item.getQuantity() <= 0) {
                return "each item needs a sku and quantity > 0";
            }
        }
        return null;
    }

    /** Devuelve true si terminó el trabajo, false si el cliente canceló o venció el deadline. */
    private boolean simulateSlowWork() {
        try {
            for (int i = 0; i < 30; i++) { // 30 x 100 ms = 3 s
                if (Context.current().isCancelled()) {
                    log.info("call cancelled (client cancel or deadline exceeded), aborting work");
                    return false;
                }
                Thread.sleep(100);
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private boolean sameContent(Order existing, CreateOrderRequest request) {
        return existing.getCustomerId().equals(request.getCustomerId())
                && existing.getItemsList().equals(request.getItemsList());
    }

    private Order storeNewOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        Order order = Order.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(request.getCustomerId())
                .addAllItems(request.getItemsList())
                .setStatus(OrderStatus.ORDER_STATUS_CREATED)
                .setCreatedAt(toTimestamp(Instant.now()))
                .build();
        orders.put(orderId, order);

        // Ciclo de vida simulado, para tener eventos que emitir en WatchOrder
        scheduler.schedule(() -> advance(orderId, OrderStatus.ORDER_STATUS_PAID), 1, TimeUnit.SECONDS);
        scheduler.schedule(() -> advance(orderId, OrderStatus.ORDER_STATUS_SHIPPED), 2, TimeUnit.SECONDS);
        return order;
    }

    // ------------------------------------------------------------------- GetOrder

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

    // ------------------------------------------------------------------ WatchOrder

    @Override
    public void watchOrder(WatchOrderRequest request, StreamObserver<OrderEvent> responseObserver) {
        ServerCallStreamObserver<OrderEvent> observer =
                (ServerCallStreamObserver<OrderEvent>) responseObserver;
        String orderId = request.getOrderId();

        synchronized (lock) {
            Order order = orders.get(orderId);
            if (order == null) {
                observer.onError(Status.NOT_FOUND
                        .withDescription("order " + orderId + " not found").asRuntimeException());
                return;
            }

            // Estado actual como primer evento
            observer.onNext(toEvent(order));
            if (isTerminal(order.getStatus())) {
                observer.onCompleted();
                return;
            }

            List<ServerCallStreamObserver<OrderEvent>> list =
                    watchers.computeIfAbsent(orderId, k -> new CopyOnWriteArrayList<>());
            list.add(observer);

            // Se ejecuta cuando el cliente cancela o vence su deadline
            observer.setOnCancelHandler(() -> {
                list.remove(observer);
                log.info("watcher cancelled for order {}", orderId);
            });
        }
    }

    private void advance(String orderId, OrderStatus next) {
        synchronized (lock) {
            Order updated = orders.computeIfPresent(orderId,
                    (id, o) -> o.toBuilder().setStatus(next).build());
            if (updated == null) {
                return;
            }
            List<ServerCallStreamObserver<OrderEvent>> list = watchers.get(orderId);
            if (list == null) {
                return;
            }
            OrderEvent event = toEvent(updated);
            boolean terminal = isTerminal(next);

            for (ServerCallStreamObserver<OrderEvent> watcher : list) {
                try {
                    if (watcher.isCancelled()) {
                        list.remove(watcher);
                        continue;
                    }
                    watcher.onNext(event);
                    if (terminal) {
                        watcher.onCompleted();
                    }
                } catch (RuntimeException e) {
                    log.warn("dropping watcher for order {}: {}", orderId, e.toString());
                    list.remove(watcher);
                }
            }
            if (terminal) {
                watchers.remove(orderId);
            }
        }
    }

    // -------------------------------------------------------------------- helpers

    private static boolean isTerminal(OrderStatus status) {
        return status == OrderStatus.ORDER_STATUS_SHIPPED
                || status == OrderStatus.ORDER_STATUS_CANCELLED;
    }

    private static OrderEvent toEvent(Order order) {
        return OrderEvent.newBuilder()
                .setOrderId(order.getOrderId())
                .setStatus(order.getStatus())
                .setOccurredAt(toTimestamp(Instant.now()))
                .build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    @Override
    public void destroy() {
        scheduler.shutdownNow();
    }
}
