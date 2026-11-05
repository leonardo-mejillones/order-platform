package com.example.orderservice;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

public class AuthServerInterceptor implements ServerInterceptor {

    private final String expectedHeader;

    public AuthServerInterceptor(String token) {
        this.expectedHeader = "Bearer " + token;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String received = headers.get(MetadataKeys.AUTHORIZATION);
        if (!expectedHeader.equals(received)) {
            call.close(Status.UNAUTHENTICATED
                    .withDescription("missing or invalid authorization header"), new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
        }
        return next.startCall(call, headers);
    }
}
