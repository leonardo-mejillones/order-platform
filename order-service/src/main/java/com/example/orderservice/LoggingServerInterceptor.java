package com.example.orderservice;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingServerInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(LoggingServerInterceptor.class);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        long start = System.nanoTime();
        String method = call.getMethodDescriptor().getFullMethodName();
        String correlationId = headers.get(MetadataKeys.CORRELATION_ID);

        ServerCall<ReqT, RespT> loggingCall =
                new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                    @Override
                    public void close(Status status, Metadata trailers) {
                        log.info("grpc method={} status={} durationMs={} correlationId={}",
                                method, status.getCode(), elapsedMs(start), correlationId);
                        super.close(status, trailers);
                    }
                };

        ServerCall.Listener<ReqT> listener = next.startCall(loggingCall, headers);

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            @Override
            public void onCancel() {
                log.info("grpc method={} status=CANCELLED_BY_CLIENT durationMs={} correlationId={}",
                        method, elapsedMs(start), correlationId);
                super.onCancel();
            }
        };
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
