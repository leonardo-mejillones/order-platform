# order-platform

Example **gRPC** and **Spring Boot** platform for order management. It is a multi-module Gradle project demonstrating all four RPC types (unary, server streaming, client streaming, and bidirectional) using a shared Protobuf contract.

## Modules

| Module | Type | Description |
| --- | --- | --- |
| `order-api` | `java-library` + `com.google.protobuf` | Defines the Protobuf/gRPC contract (`order.proto`) and generates the Java classes. Reused as a dependency. |
| `order-service` | Spring Boot | gRPC implementation/server (entry point `OrderServiceApplication`). |
| `order-client` | Spring Boot | gRPC consumer client (entry point `OrderClientApplication`). |

## Stack

- **Java 21** (toolchain configured in the root `build.gradle`)
- **Gradle 9.6.0** via wrapper (`./gradlew`)
- **gRPC 1.64.0** and **Protobuf 3.25.3** (defined in `gradle.properties`)
- **Spring Boot 3.3.4** with `io.spring.dependency-management` 1.1.6
- **JUnit 5** (`junit-bom` 6.0.0) for testing

## gRPC Contract

Defined in `order-api/src/main/proto/orders/v1/order.proto`

- Proto package: `orders.v1`
- Java package: `com.example.orders.v1`
- Service: `OrderService`
  - `CreateOrder` (unary)
  - `GetOrder` (unary)
  - `WatchOrder` (server streaming: the server pushes status updates)
  - `BulkCreateOrders` (client streaming: bulk upload)
  - `ProcessOrders` (bidirectional: commands and results with `oneof`)

Generated Java stubs and message classes are produced under `order-api/build/generated` during compilation.

## Requirements

- JDK 21 available (the build selects it via the Gradle toolchain).

## Build and Run

```bash
# Build everything (includes protobuf code generation)
./gradlew build

# Run the tests
./gradlew test

# Build only the contract/protos
./gradlew :order-api:build

# Run the server
./gradlew :order-service:bootRun

# Run the client
./gradlew :order-client:bootRun
```

## Structure

```
order-platform/
├── build.gradle              # Root config (Java 21, JUnit, subprojects)
├── settings.gradle           # Includes the three modules
├── gradle.properties         # grpc and protobuf versions
├── order-api/                # Protobuf/gRPC contract
│   └── src/main/proto/orders/v1/order.proto
├── order-service/            # gRPC server (Spring Boot)
│   └── src/main/java/com/example/orderservice/OrderServiceApplication.java
└── order-client/             # gRPC client (Spring Boot)
    └── src/main/java/com/example/orderclient/OrderClientApplication.java
```
