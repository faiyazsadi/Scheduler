package dev.faiyazsadi.scheduler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class SchedulerConfig {
    @Bean
    public String uniqueSchedulerId() {
        // Consumer Name in the Consumer Group
        return "EXECUTOR-" + UUID.randomUUID();
    }

    @Bean
    public String streamName() {
        return "JOBS";
    }

    @Bean
    public String consumerGroupName() {
        return "EXECUTORS";
    }
}
