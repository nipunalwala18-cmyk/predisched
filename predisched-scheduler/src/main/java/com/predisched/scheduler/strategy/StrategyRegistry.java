package com.predisched.scheduler.strategy;

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

    /** What factories may need from config. */
    public record Settings(long seed, double cpuWeight, double memWeight, double queueWeight) {
        public static Settings defaults() {
            return new Settings(42, 0.4, 0.2, 0.4);
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
                        settings.cpuWeight(), settings.memWeight(), settings.queueWeight()));
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
