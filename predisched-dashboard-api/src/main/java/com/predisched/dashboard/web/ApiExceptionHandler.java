package com.predisched.dashboard.web;

import com.predisched.dashboard.cluster.ClusterUnavailableException;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Cluster trouble is 503 with the reason, bad input 400: the UI shows an error state. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(ClusterUnavailableException.class)
    ResponseEntity<Map<String, Object>> unavailable(ClusterUnavailableException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "cluster unavailable", "detail", e.getMessage()));
    }

    @ExceptionHandler(StatusRuntimeException.class)
    ResponseEntity<Map<String, Object>> grpc(StatusRuntimeException e) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(Map.of("error",
                "gRPC " + e.getStatus().getCode(), "detail",
                String.valueOf(e.getStatus().getDescription())));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, Object>> bad(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
    }
}
