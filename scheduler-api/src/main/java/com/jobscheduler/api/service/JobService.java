package com.jobscheduler.api.service;

import com.jobscheduler.api.web.dto.CreateJobRequest;
import com.jobscheduler.core.ScheduleCalculator;
import com.jobscheduler.core.SchedulerProperties;
import com.jobscheduler.core.TimerOffers;
import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.domain.JobState;
import com.jobscheduler.store.domain.MissedFirePolicy;
import com.jobscheduler.store.entity.Job;
import com.jobscheduler.store.repo.JobClaimStore;
import com.jobscheduler.store.repo.JobRepository;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
public class JobService {

    private final R2dbcEntityTemplate template;
    private final JobRepository repository;
    private final JobClaimStore claimStore;
    private final SchedulerProperties props;
    private final SchedulerMetrics metrics;
    private final TimerOffers timerOffers;

    public JobService(R2dbcEntityTemplate template, JobRepository repository, JobClaimStore claimStore,
                      SchedulerProperties props, SchedulerMetrics metrics, TimerOffers timerOffers) {
        this.template = template;
        this.repository = repository;
        this.claimStore = claimStore;
        this.props = props;
        this.metrics = metrics;
        this.timerOffers = timerOffers;
    }

    public Mono<Job> create(CreateJobRequest request) {
        validate(request);
        Instant now = Instant.now();
        UUID id = UUID.randomUUID();
        Instant firstFire = ScheduleCalculator.firstFire(request.scheduleType(), request.cronExpression(),
                request.fixedRateSeconds(), request.fireAt(), now);
        if (firstFire == null) {
            throw new IllegalArgumentException("Schedule yields no future fire time");
        }

        Job job = new Job();
        job.setId(id);
        job.setType(request.type());
        job.setPayload(request.payload());
        job.setScheduleType(request.scheduleType());
        job.setCronExpression(request.cronExpression());
        job.setFixedRateSeconds(request.fixedRateSeconds());
        job.setFireAt(request.fireAt());
        job.setMissedFirePolicy(request.missedFirePolicy() != null
                ? request.missedFirePolicy() : MissedFirePolicy.FIRE_ONCE_IMMEDIATELY);
        job.setShard((short) Math.floorMod(id.hashCode(), props.shards()));
        job.setNextFireAt(firstFire);
        job.setState(JobState.PENDING);
        job.setVersion(0L);
        job.setCreatedAt(now);

        return template.insert(job).doOnSuccess(saved -> {
            metrics.jobCreated();
            // near-term jobs go straight into the wheel instead of waiting a hydration sweep
            timerOffers.offer(saved.getId(), saved.getNextFireAt());
        });
    }

    public Mono<Job> get(UUID id) {
        return repository.findById(id);
    }

    public Mono<Boolean> cancel(UUID id) {
        return claimStore.cancel(id);
    }

    private static void validate(CreateJobRequest request) {
        switch (request.scheduleType()) {
            case CRON -> {
                if (!ScheduleCalculator.isValidCron(request.cronExpression())) {
                    throw new IllegalArgumentException("A valid cronExpression is required for CRON jobs");
                }
            }
            case FIXED_RATE -> {
                if (request.fixedRateSeconds() == null || request.fixedRateSeconds() <= 0) {
                    throw new IllegalArgumentException("fixedRateSeconds must be positive for FIXED_RATE jobs");
                }
            }
            case ONE_SHOT -> {
                if (request.fireAt() == null) {
                    throw new IllegalArgumentException("fireAt is required for ONE_SHOT jobs");
                }
            }
        }
    }
}
