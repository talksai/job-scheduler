package com.jobscheduler.coordinator;

import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.repo.LeaseStore;
import com.jobscheduler.store.repo.ShardAssignmentStore;
import com.jobscheduler.store.repo.WorkerRegistryStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(CoordinationProperties.class)
public class CoordinationConfig {

    @Bean
    public PostgresLeaseCoordination coordinationService(
            LeaseStore leases, ShardAssignmentStore assignments, CoordinationProperties props,
            SchedulerMetrics metrics,
            @Value("${scheduler.worker-id:worker-local}") String workerId) {
        return new PostgresLeaseCoordination(leases, assignments, workerId, props.leaseTtlSeconds(), metrics);
    }

    @Bean
    public CoordinatorNode coordinatorNode(
            PostgresLeaseCoordination coordination, WorkerRegistryStore registry,
            ShardAssignmentStore assignments, CoordinationProperties props, SchedulerMetrics metrics,
            @Value("${scheduler.shards:16}") int totalShards) {
        return new CoordinatorNode(coordination, registry, assignments, props, metrics, totalShards);
    }

    @Bean
    public WorkerMembership workerMembership(
            WorkerRegistryStore registry, PostgresLeaseCoordination coordination,
            CoordinationProperties props, SchedulerMetrics metrics,
            @Value("${scheduler.worker-id:worker-local}") String workerId) {
        return new WorkerMembership(workerId, registry, coordination, props, metrics);
    }
}
