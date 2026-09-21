package com.predisched.scheduler;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.NodeConfig;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts a scheduler gRPC server with a single-worker dispatcher. */
public class SchedulerMain {

    private static final Logger log = LoggerFactory.getLogger(SchedulerMain.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);
        String configPath = opts.getOrDefault("--config", "configs/local.yaml");
        NodeConfig config = NodeConfig.load(Paths.get(configPath));
        String id = opts.getOrDefault("--id", config.getScheduler().getId());
        int port = Integer.parseInt(opts.getOrDefault("--port",
                String.valueOf(config.getScheduler().getPort())));

        TaskStore store = new InMemoryTaskStore();
        TaskValidator validator = new TaskValidator(config.getValidation().getMaxInputChars());
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(config.getWorker().getHost(), config.getWorker().getPort())
                .usePlaintext()
                .build();
        WorkerServiceGrpc.WorkerServiceBlockingStub workerStub =
                WorkerServiceGrpc.newBlockingStub(channel);
        Dispatcher dispatcher = new Dispatcher(store, queue, workerStub, config.getWorker().getId());
        dispatcher.start();

        Server server = ServerBuilder.forPort(port)
                .addService(new SchedulerServiceImpl(store, validator, queue))
                .build()
                .start();
        log.info("Scheduler {} listening on {}", id, port);
        System.out.println("Scheduler " + id + " listening on " + port
                + " (worker " + config.getWorker().getHost() + ":" + config.getWorker().getPort() + ")");
        server.awaitTermination();
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
                opts.put(args[i], args[i + 1]);
                i++;
            } else if (args[i].startsWith("--") && args[i].contains("=")) {
                String[] kv = args[i].split("=", 2);
                opts.put(kv[0], kv[1]);
            }
        }
        return opts;
    }
}
