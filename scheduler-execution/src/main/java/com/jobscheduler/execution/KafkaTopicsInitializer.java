package com.jobscheduler.execution;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Provisions job-events, retry and DLQ topics at startup. Runs during bean
 * initialization so topics exist before the poller's lifecycle start.
 */
@Component
public class KafkaTopicsInitializer implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicsInitializer.class);
    private static final int ATTEMPTS = 3;

    private final KafkaProperties props;

    public KafkaTopicsInitializer(KafkaProperties props) {
        this.props = props;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        List<NewTopic> topics = List.of(
                new NewTopic(props.topic(), props.partitions(), (short) 1),
                new NewTopic(props.retryTopic(), props.partitions(), (short) 1),
                new NewTopic(props.dlqTopic(), 1, (short) 1));
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers()))) {
            for (int attempt = 1; ; attempt++) {
                try {
                    admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
                    log.info("Kafka topics ready: {}, {}, {}", props.topic(), props.retryTopic(), props.dlqTopic());
                    return;
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        log.info("Kafka topics already exist");
                        return;
                    }
                    if (attempt >= ATTEMPTS) {
                        throw e;
                    }
                    log.warn("Topic creation attempt {}/{} failed: {}", attempt, ATTEMPTS, e.getCause().getMessage());
                    Thread.sleep(5000);
                }
            }
        }
    }
}
