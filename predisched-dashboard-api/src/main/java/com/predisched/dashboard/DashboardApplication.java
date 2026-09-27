package com.predisched.dashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The dashboard API (prompt 22, spec §15.10): REST reads from PostgreSQL, live state and admin
 * controls over gRPC to the primary scheduler, and a throttled STOMP stream of cluster events.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(DashboardProperties.class)
public class DashboardApplication {

    public static void main(String[] args) {
        SpringApplication.run(DashboardApplication.class, args);
    }
}
