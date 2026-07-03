package com.jobscheduler.api;

import com.jobscheduler.core.WheelSchedulerService;
import com.jobscheduler.execution.JobEventsConsumer;
import com.jobscheduler.execution.OutboxPoller;
import com.jobscheduler.store.domain.JobState;
import com.jobscheduler.store.domain.MissedFirePolicy;
import com.jobscheduler.store.domain.ScheduleType;
import com.jobscheduler.store.entity.Job;
import com.jobscheduler.store.repo.JobClaimStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * BUILD-PLAN M2 tests: duplicateClaimRejected, outboxNoDualWriteLoss,
 * consumerDedups, crashMidExecutionExactlyOnce.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class ExactlyOnceM2Test {

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
        registry.add("scheduler.hydration-interval-ms", () -> "1000");
    }

    @Autowired
    R2dbcEntityTemplate template;

    @Autowired
    DatabaseClient db;

    @Autowired
    JobClaimStore claimStore;

    @Autowired
    WheelSchedulerService wheelScheduler;

    @Autowired
    OutboxPoller outboxPoller;

    @Autowired
    JobEventsConsumer consumer;

    @Autowired
    MeterRegistry meters;

    @Test
    void duplicateClaimRejected() {
        UUID jobId = UUID.randomUUID();
        long epoch = Instant.now().getEpochSecond();

        assertThat(claimStore.tryClaim(jobId, epoch, "worker-1", 60).block())
                .as("first claim wins").isTrue();
        assertThat(claimStore.tryClaim(jobId, epoch, "worker-2", 60).block())
                .as("live lease blocks a second claimant").isFalse();

        claimStore.markFired(jobId, epoch).block();
        assertThat(claimStore.tryClaim(jobId, epoch, "worker-2", 60).block())
                .as("FIRED is terminal — never re-claimable").isFalse();

        // a crashed worker's claim: lease already expired
        long epoch2 = epoch + 1;
        assertThat(claimStore.tryClaim(jobId, epoch2, "worker-1", -1).block()).isTrue();
        assertThat(claimStore.tryClaim(jobId, epoch2, "worker-2", 60).block())
                .as("expired lease is re-claimable").isTrue();

        Map<String, Object> row = db.sql("SELECT claimed_by, attempt FROM execution " +
                        "WHERE job_id = :id AND fire_epoch = :epoch")
                .bind("id", jobId).bind("epoch", epoch2).fetch().one().block();
        assertThat(row.get("claimed_by")).isEqualTo("worker-2");
        assertThat(row.get("attempt")).isEqualTo(2);
    }

    @Test
    void outboxNoDualWriteLoss() {
        // the "worker died before it could publish" window: poller down
        outboxPoller.stop();
        try {
            Job job = oneShot("outbox-loss", Instant.now().plusSeconds(1));
            template.insert(job).block(Duration.ofSeconds(5));
            String key = job.getId().toString();

            // the fire tx commits: execution FIRED + event durable in the outbox
            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(executionStatuses(job.getId())).containsExactly("FIRED");
                assertThat(unpublishedOutboxRows(key)).isEqualTo(1L);
            });
            // ... but nothing reached Kafka yet
            assertThat(eventsOnTopicFor(key, Duration.ofSeconds(3)))
                    .as("no event on the broker while the publisher is down").isEmpty();
        } finally {
            outboxPoller.start();
        }

        Job job2 = latestJobOfType("outbox-loss");
        String key = job2.getId().toString();
        // recovery: the restarted poller publishes the parked row — exactly once
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(unpublishedOutboxRows(key)).isZero());
        assertThat(eventsOnTopicFor(key, Duration.ofSeconds(5))).hasSize(1);
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(executionStatuses(job2.getId())).containsExactly("COMPLETED"));
    }

    @Test
    void consumerDedups() {
        Job job = oneShot("dedup", Instant.now().plusSeconds(1));
        template.insert(job).block(Duration.ofSeconds(5));
        String key = job.getId().toString();

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(executionStatuses(job.getId())).containsExactly("COMPLETED"));

        List<ConsumerRecord<String, String>> delivered = eventsOnTopicFor(key, Duration.ofSeconds(5));
        assertThat(delivered).hasSize(1);

        double dedupBefore = meters.get("scheduler.events.dedup").counter().count();
        // replay the exact same event — a duplicate delivery
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            producer.send(new ProducerRecord<>("job-events", key, delivered.getFirst().value()));
            producer.flush();
        }

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(meters.get("scheduler.events.dedup").counter().count())
                        .as("replay absorbed as a dedup hit").isGreaterThan(dedupBefore));
        assertThat(executionStatuses(job.getId()))
                .as("the effect applied exactly once").containsExactly("COMPLETED");
        Long processed = db.sql("SELECT count(*) AS c FROM processed_event WHERE event_key LIKE :prefix")
                .bind("prefix", key + ":%").map(r -> r.get("c", Long.class)).one().block();
        assertThat(processed).isEqualTo(1L);
    }

    @Test
    void crashMidExecutionExactlyOnce() throws Exception {
        int jobCount = 30;
        Instant base = Instant.now().plusSeconds(2);
        List<Job> jobs = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            jobs.add(oneShot("crash", base.plusMillis(i * 65L))); // spread over ~2s
        }
        Flux.fromIterable(jobs).flatMap(template::insert, 8).blockLast(Duration.ofSeconds(10));
        UUID[] ids = jobs.stream().map(Job::getId).toArray(UUID[]::new);

        // kill the worker mid-stream: some events consumed, some published but
        // unconsumed, some parked in the outbox, some jobs not yet fired
        Thread.sleep(2_800);
        wheelScheduler.stop();
        outboxPoller.stop();
        consumer.stop();
        Thread.sleep(500);
        consumer.start();
        outboxPoller.start();
        wheelScheduler.start();

        // reconciliation: every path converges to exactly-once effect
        Awaitility.await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            Long completedJobs = db.sql("SELECT count(*) AS c FROM job WHERE id = ANY(:ids) AND state = 'COMPLETED'")
                    .bind("ids", ids).map(r -> r.get("c", Long.class)).one().block();
            assertThat(completedJobs).as("all jobs completed").isEqualTo((long) jobCount);

            Long executions = db.sql("SELECT count(*) AS c FROM execution WHERE job_id = ANY(:ids)")
                    .bind("ids", ids).map(r -> r.get("c", Long.class)).one().block();
            assertThat(executions).as("exactly one execution per job").isEqualTo((long) jobCount);

            Long completedExecutions = db.sql("SELECT count(*) AS c FROM execution " +
                            "WHERE job_id = ANY(:ids) AND status = 'COMPLETED'")
                    .bind("ids", ids).map(r -> r.get("c", Long.class)).one().block();
            assertThat(completedExecutions).as("every execution's effect applied").isEqualTo((long) jobCount);

            Long published = db.sql("SELECT count(*) AS c FROM outbox " +
                            "WHERE aggregate_id = ANY(:aggIds) AND published_at IS NOT NULL")
                    .bind("aggIds", jobs.stream().map(j -> j.getId().toString()).toArray(String[]::new))
                    .map(r -> r.get("c", Long.class)).one().block();
            assertThat(published).as("every outbox row published, none ghosted").isEqualTo((long) jobCount);
        });
    }

    private List<String> executionStatuses(UUID jobId) {
        return db.sql("SELECT status FROM execution WHERE job_id = :id ORDER BY fire_epoch")
                .bind("id", jobId).map(r -> r.get("status", String.class)).all()
                .collectList().block();
    }

    private Long unpublishedOutboxRows(String aggregateId) {
        return db.sql("SELECT count(*) AS c FROM outbox WHERE aggregate_id = :agg AND published_at IS NULL")
                .bind("agg", aggregateId).map(r -> r.get("c", Long.class)).one().block();
    }

    private Job latestJobOfType(String type) {
        UUID id = db.sql("SELECT id FROM job WHERE type = :type ORDER BY created_at DESC LIMIT 1")
                .bind("type", type).map(r -> r.get("id", UUID.class)).one().block();
        Job job = new Job();
        job.setId(id);
        return job;
    }

    private List<ConsumerRecord<String, String>> eventsOnTopicFor(String key, Duration window) {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        try (KafkaConsumer<String, String> observer = new KafkaConsumer<>(observerProperties())) {
            observer.subscribe(List.of("job-events"));
            long deadline = System.currentTimeMillis() + window.toMillis();
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : observer.poll(Duration.ofMillis(300))) {
                    if (key.equals(record.key())) {
                        matches.add(record);
                    }
                }
            }
        }
        return matches;
    }

    private static Job oneShot(String type, Instant fireAt) {
        Job job = new Job();
        UUID id = UUID.randomUUID();
        job.setId(id);
        job.setType(type);
        job.setPayload("{}");
        job.setScheduleType(ScheduleType.ONE_SHOT);
        job.setFireAt(fireAt);
        job.setNextFireAt(fireAt);
        job.setMissedFirePolicy(MissedFirePolicy.FIRE_ONCE_IMMEDIATELY);
        job.setShard((short) Math.floorMod(id.hashCode(), 16));
        job.setState(JobState.PENDING);
        job.setVersion(0L);
        job.setCreatedAt(Instant.now());
        return job;
    }

    private static Map<String, Object> observerProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "m2-observer-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private static Map<String, Object> producerProperties() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }
}
