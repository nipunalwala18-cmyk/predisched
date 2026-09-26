package com.predisched.scheduler.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;

class StrategyRegistryTest {

    @Test
    void theStandardRegistryHasTheFourStrategies() {
        assertEquals(Set.of("round_robin", "random", "least_loaded", "resource_aware"),
                StrategyRegistry.standard().names());
    }

    @Test
    void anUnknownNameFailsAndListsTheValidOnes() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
                StrategyRegistry.standard().create("fastest", StrategyRegistry.Settings.defaults()));
        assertTrue(error.getMessage().contains("unknown scheduling.strategy 'fastest'"),
                error.getMessage());
        assertTrue(error.getMessage().contains(
                "[round_robin, random, least_loaded, resource_aware]"), error.getMessage());
    }

    @Test
    void aNameCannotBeRegisteredTwice() {
        assertThrows(IllegalArgumentException.class, () -> StrategyRegistry.standard()
                .register("random", settings -> new RoundRobinStrategy()));
    }
}
