package com.jobscheduler.execution;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "scheduler.kafka")
public record KafkaProperties(
        @DefaultValue("localhost:9092") String bootstrapServers,
        @DefaultValue("job-events") String topic,
        @DefaultValue("job-events.retry") String retryTopic,
        @DefaultValue("job-events.dlq") String dlqTopic,
        @DefaultValue("job-scheduler") String consumerGroup,
        @DefaultValue("3") int maxAttempts,
        @DefaultValue("8") int partitions) {
}
