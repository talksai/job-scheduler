package com.jobscheduler.api;

import com.jobscheduler.coordinator.CoordinationProperties;
import com.jobscheduler.coordinator.CoordinatorNode;
import com.jobscheduler.coordinator.PostgresLeaseCoordination;
import com.jobscheduler.coordinator.WorkerMembership;
import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.domain.JobState;
import com.jobscheduler.store.domain.MissedFirePolicy;
import com.jobscheduler.store.domain.ScheduleType;
import com.jobscheduler.store.entity.Job;
import com.jobscheduler.store.repo.LeaseStore;
import com.jobscheduler.store.repo.ShardAssignmentStore;
import com.jobscheduler.store.repo.ShardAssignmentStore.ShardAssignmentRow;
import com.jobscheduler.store.repo.WorkerRegistryStore;
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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * BUILD-PLAN M3 tests: staleTokenFenced, rebalanceOnWorkerJoinLeave,
 * workerFailoverNoLoss, coordinatorFailoverNoDoubleFire.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class CoordinationM3Test {

    private static final int SHARDS = 16;

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
        registry.add("scheduler.coordination.election-interval-ms", () -> "500");
        registry.add("scheduler.coordination.lease-ttl-seconds", () -> "3");
        registry.add("scheduler.coordination.heartbeat-interval-ms", () -> "500");
        registry.add("scheduler.coordination.worker-ttl-seconds", () -> "3");
        registry.add("scheduler.coordination.assignment-poll-ms", () -> "500");
    }

    @Autowired
    R2dbcEntityTemplate template;

    @Autowired
    DatabaseClient db;

    @Autowired
    LeaseStore leaseStore;

    @Autowired
    ShardAssignmentStore assignmentStore;

    @Autowired
    WorkerRegistryStore registryStore;

    @Autowired
    CoordinationProperties coordinationProps;

    @Autowired
    SchedulerMetrics metrics;

    @Autowired
    CoordinatorNode coordinatorNode;

    @Autowired
    WorkerMembership membership;

    @Autowired
    PostgresLeaseCoordination coordination;

    @Test
    void staleTokenFenced() throws Exception {
        db.sql("INSERT INTO coordinator_lease (role, epoch) VALUES ('fence-test', 0) ON CONFLICT DO NOTHING")
                .fetch().rowsUpdated().block();

        Long e1 = leaseStore.tryAcquire("fence-test", "c1", 1).block();
        assertThat(e1).isNotNull();
        assertThat(leaseStore.renew("fence-test", "c1", e1, 1).block())
                .as("holder renews a live lease").isTrue();

        Thread.sleep(1_300); // let c1's lease expire — simulates a paused/dead holder
        Long e2 = leaseStore.tryAcquire("fence-test", "c2", 30).block();
        assertThat(e2).as("new holder gets a strictly higher fencing token").isEqualTo(e1 + 1);
        assertThat(leaseStore.renew("fence-test", "c1", e1, 30).block())
                .as("the deposed holder cannot renew its way back in").isFalse();

        // fencing on writes: shard 99 is outside the managed 0..15 range
        assertThat(assignmentStore.assign(99, "w-new", e2).block()).isTrue();
        assertThat(assignmentStore.assign(99, "w-old", e1).block())
                .as("a write carrying the stale token is rejected").isFalse();
        assertThat(assignmentStore.assign(99, "w-new2", e2).block())
                .as("the current token still writes").isTrue();

        // isFenced: an observer that lost its campaign learns the incumbent epoch
        PostgresLeaseCoordination observer =
                new PostgresLeaseCoordination(leaseStore, assignmentStore, "observer", 3);
        assertThat(observer.campaign("fence-test").block()).as("campaign against live lease loses").isNull();
        assertThat(observer.isFenced(e1)).isTrue();
        assertThat(observer.isFenced(e2)).isFalse();
    }

    @Test
    void rebalanceOnWorkerJoinLeave() {
        String appWorker = membership.workerId();
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(owners()).as("single worker owns everything")
                        .containsExactly(appWorker));

        WorkerMembership joiner = new WorkerMembership("worker-join", registryStore,
                coordination, coordinationProps, metrics);
        joiner.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                List<ShardAssignmentRow> all = assignmentStore.all().collectList().block().stream()
                        .filter(row -> row.shard() < SHARDS).toList();
                long joinerShards = all.stream().filter(r -> r.workerId().equals("worker-join")).count();
                long appShards = all.stream().filter(r -> r.workerId().equals(appWorker)).count();
                assertThat(all).hasSize(SHARDS);
                assertThat(joinerShards).as("join splits the shard space").isEqualTo(SHARDS / 2);
                assertThat(appShards).isEqualTo(SHARDS / 2);
            });
        } finally {
            joiner.stop();
        }

        // leave: heartbeat goes silent -> coordinator hands everything back
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(owners()).containsExactly(appWorker));
    }

    @Test
    void workerFailoverNoLoss() throws Exception {
        WorkerMembership doomed = new WorkerMembership("worker-doomed", registryStore,
                coordination, coordinationProps, metrics);
        doomed.start();
        Set<Integer> doomedShards;
        try {
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
                    !assignmentStore.assignedTo("worker-doomed").collectList().block().isEmpty());
            doomedShards = assignmentStore.assignedTo("worker-doomed").collectList().block()
                    .stream().map(ShardAssignmentRow::shard).collect(Collectors.toSet());

            // jobs pinned to the doomed worker's shards, due in 1s
            List<Job> jobs = new ArrayList<>();
            List<Integer> shardList = List.copyOf(doomedShards);
            for (int i = 0; i < 6; i++) {
                jobs.add(pinnedOneShot("worker-failover", Instant.now().plusSeconds(1),
                        shardList.get(i % shardList.size())));
            }
            Flux.fromIterable(jobs).flatMap(template::insert).blockLast(Duration.ofSeconds(10));

            // partition discipline: the surviving worker must NOT fire foreign shards
            Thread.sleep(4_000);
            assertThat(pendingCount("worker-failover"))
                    .as("jobs on another worker's shards stay untouched").isEqualTo(6L);
        } finally {
            doomed.stop(); // the worker dies mid-ownership, jobs unfired
        }

        // coordinator reassigns after the worker TTL; survivor catches up the overdue fires
        Awaitility.await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            Long completed = db.sql("SELECT count(*) AS c FROM job WHERE type = 'worker-failover' AND state = 'COMPLETED'")
                    .map(r -> r.get("c", Long.class)).one().block();
            assertThat(completed).as("no fire lost after worker death").isEqualTo(6L);
            Long executions = db.sql("SELECT count(*) AS c FROM execution e JOIN job j ON j.id = e.job_id " +
                            "WHERE j.type = 'worker-failover'")
                    .map(r -> r.get("c", Long.class)).one().block();
            assertThat(executions).as("exactly one execution per job").isEqualTo(6L);
        });
    }

    @Test
    void coordinatorFailoverNoDoubleFire() {
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(coordinatorNode::isLeader);
        long epochBefore = leaseStore.read(CoordinatorNode.COORDINATOR_ROLE).block().epoch();

        // a recurring job that must keep firing exactly-once through the failover
        Job job = new Job();
        UUID id = UUID.randomUUID();
        job.setId(id);
        job.setType("coordinator-failover");
        job.setPayload("{}");
        job.setScheduleType(ScheduleType.FIXED_RATE);
        job.setFixedRateSeconds(2L);
        job.setNextFireAt(Instant.now().plusSeconds(1));
        job.setMissedFirePolicy(MissedFirePolicy.FIRE_ALL);
        job.setShard((short) Math.floorMod(id.hashCode(), SHARDS));
        job.setState(JobState.PENDING);
        job.setVersion(0L);
        job.setCreatedAt(Instant.now());
        template.insert(job).block(Duration.ofSeconds(5));

        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> executionCount(id) >= 2);

        // the coordinator dies (stops renewing); a rival campaigns for the lease
        coordinatorNode.stop();
        PostgresLeaseCoordination rivalCoordination =
                new PostgresLeaseCoordination(leaseStore, assignmentStore, "rival", coordinationProps.leaseTtlSeconds());
        CoordinatorNode rival = new CoordinatorNode(rivalCoordination, registryStore, assignmentStore,
                coordinationProps, metrics, SHARDS);
        rival.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                LeaseStore.LeaseRow lease = leaseStore.read(CoordinatorNode.COORDINATOR_ROLE).block();
                assertThat(lease.holder()).as("rival wins within the lease TTL").isEqualTo("rival");
                assertThat(lease.epoch()).as("fencing token strictly increases").isEqualTo(epochBefore + 1);
            });

            // firing continues under the new coordinator...
            long atFailover = executionCount(id);
            Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> executionCount(id) >= atFailover + 2);

            // ...and the deposed coordinator is fenced out of the assignment table
            assertThat(assignmentStore.assign(0, "evil-worker", epochBefore).block())
                    .as("the old epoch can no longer write").isFalse();

            // exactly-once held throughout: one execution row per fire instant
            Long duplicates = db.sql("SELECT count(*) AS c FROM (SELECT fire_epoch FROM execution " +
                            "WHERE job_id = :id GROUP BY fire_epoch HAVING count(*) > 1) d")
                    .bind("id", id).map(r -> r.get("c", Long.class)).one().block();
            assertThat(duplicates).as("no double-fire across the failover").isZero();
        } finally {
            rival.stop();
            coordinatorNode.start();
        }
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(coordinatorNode::isLeader);
        db.sql("UPDATE job SET state = 'CANCELLED', next_fire_at = NULL WHERE id = :id")
                .bind("id", id).fetch().rowsUpdated().block();
    }

    private List<String> owners() {
        return assignmentStore.all().collectList().block().stream()
                .filter(row -> row.shard() < SHARDS) // staleTokenFenced probes live on shard 99
                .map(ShardAssignmentRow::workerId).distinct().sorted().toList();
    }

    private long executionCount(UUID jobId) {
        return db.sql("SELECT count(*) AS c FROM execution WHERE job_id = :id")
                .bind("id", jobId).map(r -> r.get("c", Long.class)).one().block();
    }

    private Long pendingCount(String type) {
        return db.sql("SELECT count(*) AS c FROM job WHERE type = :type AND state = 'PENDING'")
                .bind("type", type).map(r -> r.get("c", Long.class)).one().block();
    }

    private static Job pinnedOneShot(String type, Instant fireAt, int shard) {
        Job job = new Job();
        job.setId(UUID.randomUUID());
        job.setType(type);
        job.setPayload("{}");
        job.setScheduleType(ScheduleType.ONE_SHOT);
        job.setFireAt(fireAt);
        job.setNextFireAt(fireAt);
        job.setMissedFirePolicy(MissedFirePolicy.FIRE_ONCE_IMMEDIATELY);
        job.setShard((short) shard);
        job.setState(JobState.PENDING);
        job.setVersion(0L);
        job.setCreatedAt(Instant.now());
        return job;
    }
}
