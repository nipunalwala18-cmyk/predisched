package com.predisched.scheduler.strategy;

import com.predisched.common.NodeConfig;
import com.predisched.scheduler.prediction.PredictionClient;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Strategy name to factory. {@link #standard()} fills it once with the built-in strategies; a new
 * strategy is one more {@link #register} call. An unknown name fails at startup and lists the
 * valid ones, rather than falling back to something silently.
 */
public final class StrategyRegistry {

    /**
     * What factories may need from config. {@code prediction} is where the prediction server is
     * (prompt 17) and {@code predictive} the cost-function settings (prompt 18).
     */
    public record Settings(long seed, double cpuWeight, double memWeight, double queueWeight,
            NodeConfig.PredictionConfig prediction, PredictiveStrategy.Settings predictive) {

        public Settings(long seed, double cpuWeight, double memWeight, double queueWeight) {
            this(seed, cpuWeight, memWeight, queueWeight, new NodeConfig.PredictionConfig(),
                    PredictiveStrategy.Settings.defaults());
        }

        public static Settings defaults() {
            return new Settings(42, 0.4, 0.2, 0.4);
        }

        /** Every field from the node config, as the scheduler starts with it. */
        public static Settings from(NodeConfig config) {
            NodeConfig.SchedulingConfig s = config.getScheduling();
            return new Settings(s.getSeed(), s.getCpuWeight(), s.getMemWeight(),
                    s.getQueueWeight(), config.getPrediction(),
                    new PredictiveStrategy.Settings(s.getLambda(), s.getOverloadThreshold(),
                            s.getHighPriority(), s.getHighPriorityOverloadThreshold()));
        }
    }

    private final Map<String, Function<Settings, SchedulingStrategy>> factories =
            new LinkedHashMap<>();

    public static StrategyRegistry standard() {
        return new StrategyRegistry()
                .register("round_robin", settings -> new RoundRobinStrategy())
                .register("random", settings -> new RandomStrategy(settings.seed()))
                .register("least_loaded", settings -> new LeastLoadedStrategy())
                .register("resource_aware", settings -> new ResourceAwareStrategy(
                        settings.cpuWeight(), settings.memWeight(), settings.queueWeight()))
                .register("predictive", StrategyRegistry::predictive);
    }

    /**
     * The predictive strategy with its own client to the prediction server. With
     * {@code prediction.enabled: false} it still runs, falling back to least loaded on every
     * decision with that reason, so a misconfiguration shows up in the decisions, not as a crash.
     */
    private static SchedulingStrategy predictive(Settings settings) {
        NodeConfig.PredictionConfig prediction = settings.prediction();
        if (prediction == null || !prediction.isEnabled()) {
            return new PredictiveStrategy(null, settings.predictive(),
                    "prediction.enabled is false");
        }
        PredictionClient client = PredictionClient.create(prediction);
        client.warmUp(2_000);
        return new PredictiveStrategy(client::predict, settings.predictive());
    }

    public synchronized StrategyRegistry register(
            String name, Function<Settings, SchedulingStrategy> factory) {
        if (factories.putIfAbsent(name, factory) != null) {
            throw new IllegalArgumentException("strategy already registered: " + name);
        }
        return this;
    }

    public synchronized SchedulingStrategy create(String name, Settings settings) {
        Function<Settings, SchedulingStrategy> factory = factories.get(name);
        if (factory == null) {
            throw new IllegalArgumentException("unknown scheduling.strategy '" + name
                    + "'; valid strategies are " + factories.keySet());
        }
        return factory.apply(settings);
    }

    public synchronized Set<String> names() {
        return Set.copyOf(factories.keySet());
    }
}
