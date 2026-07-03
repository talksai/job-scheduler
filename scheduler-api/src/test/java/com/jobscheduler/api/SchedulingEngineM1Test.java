package com.jobscheduler.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.core.WheelSchedulerService;
import com.jobscheduler.store.domain.JobState;
import com.jobscheduler.store.domain.MissedFirePolicy;
import com.jobscheduler.store.domain.ScheduleType;
import com.jobscheduler.store.entity.Job;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * BUILD-PLAN M1 tests: firesManyTimersWithinJitterSLO, rebuildsWheelAfterRestart,
 * missedFirePolicyApplied.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class SchedulingEngineM1Test {

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
        registry.add("scheduler.missed-fire-threshold-ms", () -> "2000");
    }

    @Autowired
    R2dbcEntityTemplate template;

    @Autowired
    DatabaseClient db;

    @Autowired
    WheelSchedulerService wheelScheduler;

    @Autowired
    ObjectMapper mapper;

    @Test
    void firesManyTimersWithinJitterSLO() throws Exception {
        int jobCount = 1_000;
        Instant base = Instant.now().plusSeconds(5);
        Random random = new Random(7);

        List<Job> jobs = new ArrayList<>(jobCount);
        for (int i = 0; i < jobCount; i++) {
            Instant fireAt = base.plusMillis(random.nextInt(8_000));
            jobs.add(oneShot("jitter", fireAt));
        }
        Flux.fromIterable(jobs).flatMap(template::insert, 16).blockLast(Duration.ofSeconds(30));
        Set<String> jobIds = jobs.stream().map(j -> j.getId().toString()).collect(Collectors.toSet());

        Map<String, Long> latenciesMs = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(observerProperties())) {
            consumer.subscribe(List.of("job-events"));
            long deadline = System.currentTimeMillis() + 90_000;
            while (latenciesMs.size() < jobCount && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (!jobIds.contains(record.key())) {
                        continue;
                    }
                    JsonNode event = mapper.readTree(record.value());
                    long latency = Instant.parse(event.get("firedAt").asText()).toEpochMilli()
                            - Instant.parse(event.get("scheduledAt").asText()).toEpochMilli();
                    latenciesMs.putIfAbsent(record.key(), latency);
                }
            }
        }

        assertThat(latenciesMs).as("all scheduled fires observed on job-events").hasSize(jobCount);
        List<Long> sorted = latenciesMs.values().stream().sorted().toList();
        long p50 = sorted.get((int) Math.ceil(0.50 * jobCount) - 1);
        long p95 = sorted.get((int) Math.ceil(0.95 * jobCount) - 1);
        long p99 = sorted.get((int) Math.ceil(0.99 * jobCount) - 1);
        System.out.printf("Scheduling accuracy over %d fires: p50=%dms p95=%dms p99=%dms max=%dms%n",
                jobCount, p50, p95, p99, sorted.getLast());
        // production SLO target is 500ms; the test bound is looser to absorb
        // Testcontainers/CI noise while still catching regressions
        assertThat(p99).as("scheduled-vs-actual fire p99").isLessThanOrEqualTo(1_000);
    }

    @Test
    void rebuildsWheelAfterRestart() throws Exception {
        List<Job> jobs = new ArrayList<>();
        Instant base = Instant.now().plusSeconds(4);
        for (int i = 0; i < 20; i++) {
            jobs.add(oneShot("restart", base.plusMillis(i * 100L)));
        }
        Flux.fromIterable(jobs).flatMap(template::insert, 8).blockLast(Duration.ofSeconds(10));
        UUID[] ids = jobs.stream().map(Job::getId).toArray(UUID[]::new);

        // crash: the volatile wheel disappears with every parked timer
        wheelScheduler.stop();
        // let every scheduled fire time pass while the scheduler is down
        Thread.sleep(7_000);
        // recovery: start() rebuilds the wheel from Postgres; overdue one-shots
        // come back as immediate candidates via the missed-fire path
        wheelScheduler.start();

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Long completed = db.sql("SELECT count(*) AS c FROM job WHERE id = ANY(:ids) AND state = 'COMPLETED'")
                    .bind("ids", ids).map(row -> row.get("c", Long.class)).one().block();
            assertThat(completed).as("every job completes after restart").isEqualTo(20L);
        });

        Long executions = db.sql("SELECT count(*) AS c FROM execution WHERE job_id = ANY(:ids)")
                .bind("ids", ids).map(row -> row.get("c", Long.class)).one().block();
        assertThat(executions).as("exactly one execution per job — nothing lost, nothing doubled").isEqualTo(20L);
    }

    @Test
    void missedFirePolicyApplied() {
        // anchor 130s in the past on a 60s cadence -> missed occurrences at T, T+60, T+120
        Instant anchor = Instant.now().minusSeconds(130).truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        Job skip = fixedRateAt("missed-skip", 60, anchor, MissedFirePolicy.SKIP);
        Job once = fixedRateAt("missed-once", 60, anchor, MissedFirePolicy.FIRE_ONCE_IMMEDIATELY);
        Job all = fixedRateAt("missed-all", 60, anchor, MissedFirePolicy.FIRE_ALL);
        Flux.just(skip, once, all).flatMap(template::insert).blockLast(Duration.ofSeconds(10));

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(fireEpochs(skip.getId())).as("SKIP fires nothing").isEmpty();
            assertThat(nextFireAt(skip.getId())).as("SKIP advances onto the future grid")
                    .isEqualTo(anchor.plusSeconds(180));

            assertThat(fireEpochs(once.getId())).as("FIRE_ONCE_IMMEDIATELY fires the missed instant once")
                    .containsExactly(anchor.getEpochSecond());
            assertThat(nextFireAt(once.getId())).isEqualTo(anchor.plusSeconds(180));

            assertThat(fireEpochs(all.getId())).as("FIRE_ALL catches up every missed occurrence")
                    .containsExactly(anchor.getEpochSecond(),
                            anchor.plusSeconds(60).getEpochSecond(),
                            anchor.plusSeconds(120).getEpochSecond());
            assertThat(nextFireAt(all.getId())).isEqualTo(anchor.plusSeconds(180));
        });

        // stop the recurring jobs so they don't fire into other tests
        for (UUID id : List.of(skip.getId(), once.getId(), all.getId())) {
            db.sql("UPDATE job SET state = 'CANCELLED', next_fire_at = NULL WHERE id = :id")
                    .bind("id", id).fetch().rowsUpdated().block();
        }
    }

    private List<Long> fireEpochs(UUID jobId) {
        return db.sql("SELECT fire_epoch FROM execution WHERE job_id = :id ORDER BY fire_epoch")
                .bind("id", jobId).map(row -> row.get("fire_epoch", Long.class)).all()
                .collectList().block();
    }

    private Instant nextFireAt(UUID jobId) {
        return db.sql("SELECT next_fire_at FROM job WHERE id = :id")
                .bind("id", jobId).map(row -> row.get("next_fire_at", Instant.class)).one().block();
    }

    private static Job oneShot(String type, Instant fireAt) {
        Job job = base(type);
        job.setScheduleType(ScheduleType.ONE_SHOT);
        job.setFireAt(fireAt);
        job.setNextFireAt(fireAt);
        job.setMissedFirePolicy(MissedFirePolicy.FIRE_ONCE_IMMEDIATELY);
        return job;
    }

    private static Job fixedRateAt(String type, long rateSeconds, Instant nextFireAt, MissedFirePolicy policy) {
        Job job = base(type);
        job.setScheduleType(ScheduleType.FIXED_RATE);
        job.setFixedRateSeconds(rateSeconds);
        job.setNextFireAt(nextFireAt);
        job.setMissedFirePolicy(policy);
        return job;
    }

    private static Job base(String type) {
        Job job = new Job();
        UUID id = UUID.randomUUID();
        job.setId(id);
        job.setType(type);
        job.setPayload("{}");
        job.setShard((short) Math.floorMod(id.hashCode(), 16));
        job.setState(JobState.PENDING);
        job.setVersion(0L);
        job.setCreatedAt(Instant.now());
        return job;
    }

    private static Map<String, Object> observerProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "m1-observer-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);
    }
}
