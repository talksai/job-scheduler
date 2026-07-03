package com.jobscheduler.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** M0 DoD: a job created via REST fires and emits a Kafka event (BUILD-PLAN M0). */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30000")
@Testcontainers
class CreateJobFiresAndEmitsEventTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://" + POSTGRES.getHost() + ":"
                + POSTGRES.getMappedPort(5432) + "/" + POSTGRES.getDatabaseName());
        registry.add("spring.r2dbc.username", POSTGRES::getUsername);
        registry.add("spring.r2dbc.password", POSTGRES::getPassword);
        registry.add("spring.flyway.url", POSTGRES::getJdbcUrl);
        registry.add("spring.flyway.user", POSTGRES::getUsername);
        registry.add("spring.flyway.password", POSTGRES::getPassword);
        registry.add("scheduler.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("scheduler.poll-interval-ms", () -> "200");
    }

    @Autowired
    WebTestClient web;

    @Test
    void createJobFiresAndEmitsEvent() {
        Map<String, Object> request = Map.of(
                "type", "email",
                "payload", "{\"to\":\"user@example.com\"}",
                "scheduleType", "ONE_SHOT",
                "fireAt", Instant.now().plusSeconds(1).toString());

        JsonNode created = web.post().uri("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(JsonNode.class)
                .returnResult().getResponseBody();

        assertThat(created).isNotNull();
        String jobId = created.get("id").asText();
        assertThat(created.get("state").asText()).isEqualTo("PENDING");

        assertThat(pollForJobEvent(jobId))
                .as("job-events record for job %s", jobId)
                .isTrue();

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                web.get().uri("/api/jobs/{id}", jobId)
                        .exchange()
                        .expectStatus().isOk()
                        .expectBody()
                        .jsonPath("$.state").isEqualTo("COMPLETED"));
    }

    private boolean pollForJobEvent(String jobId) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(observerProperties())) {
            consumer.subscribe(List.of("job-events"));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (jobId.equals(record.key()) && record.value().contains(jobId)
                            && record.value().contains("fireEpoch")) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static Map<String, Object> observerProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-observer-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }
}
