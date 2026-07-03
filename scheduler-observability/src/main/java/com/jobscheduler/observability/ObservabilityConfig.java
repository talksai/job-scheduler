package com.jobscheduler.observability;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ObservabilityConfig {

    @Bean
    public SchedulerMetrics schedulerMetrics(MeterRegistry registry) {
        return new SchedulerMetrics(registry);
    }
}
