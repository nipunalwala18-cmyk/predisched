package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskExecutor;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;

/**
 * Calls a URL (in demos, the mock HTTP service in {@code scripts/mock-http.py}) with a deadline.
 * Network-bound: good for demonstrating timeouts and retries (spec §7.2). A non-2xx status is a
 * task failure, so {@code /fail?rate=1.0} deterministically fails and {@code rate=0.0} succeeds.
 */
public class HttpTaskExecutor implements TaskExecutor {

    private final HttpClient http;

    public HttpTaskExecutor() {
        this.http = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    HttpTaskExecutor(HttpClient http) {
        this.http = http;
    }

    @Override
    public TaskType type() {
        return TaskType.HTTP_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.NETWORK_BOUND;
    }

    @Override
    public ExecutionResult execute(String input) {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        Map<String, String> params = InputParser.parse(input);
        String url = params.get("url");
        long timeoutMs = Long.parseLong(params.get("timeout"));
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .GET()
                    .build();
            HttpResponse<byte[]> response =
                    http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            byte[] body = response.body() == null ? new byte[0] : response.body();
            String checksum = HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(body));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                return ExecutionResult.failure("GET " + url + " returned status "
                        + response.statusCode() + " (" + body.length + " bytes)");
            }
            return ExecutionResult.success("url=" + url + " status=" + response.statusCode()
                    + " bytes=" + body.length + " sha256=" + checksum);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.util.concurrent.CancellationException("interrupted while fetching " + url);
        } catch (IllegalArgumentException e) {
            return ExecutionResult.failure("bad url '" + url + "': " + e.getMessage());
        } catch (Exception e) {
            return ExecutionResult.failure("GET " + url + " failed: " + e.getMessage());
        }
    }
}
