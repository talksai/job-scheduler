package com.jobscheduler.core;

import com.jobscheduler.store.entity.Job;
import com.jobscheduler.store.repo.JobClaimStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

/**
 * M0 due-detection: poll the idx_due partial index, claim each fire inside one
 * transaction (SKIP LOCKED + execution INSERT ON CONFLICT), publish after commit.
 * The hierarchical timing wheel replaces the poll loop in M1.
 */
@Component
public class DuePollerService implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DuePollerService.class);

    private final JobClaimStore store;
    private final TransactionalOperator tx;
    private final FirePublisher publisher;
    private final SchedulerProperties props;

    private volatile Disposable subscription;

    public DuePollerService(JobClaimStore store, TransactionalOperator tx,
                            FirePublisher publisher, SchedulerProperties props) {
        this.store = store;
        this.tx = tx;
        this.publisher = publisher;
        this.props = props;
    }

    @Override
    public void start() {
        if (!props.pollerEnabled()) {
            log.info("Due poller disabled by configuration");
            return;
        }
        subscription = Flux.interval(Duration.ofMillis(props.pollIntervalMs()))
                .onBackpressureDrop()
                .concatMap(tick -> pollOnce()
                        .onErrorResume(e -> {
                            log.error("Due poll failed", e);
                            return Mono.just(0);
                        }), 1)
                .subscribe();
        log.info("Due poller started: interval={}ms batch={}", props.pollIntervalMs(), props.batchSize());
    }

    Mono<Integer> pollOnce() {
        return store.lockDueJobs(props.batchSize())
                .concatMap(this::claimAndAdvance)
                .collectList()
                .as(tx::transactional)
                .flatMap(fired -> fired.isEmpty()
                        ? Mono.just(0)
                        : publisher.publishAll(fired).thenReturn(fired.size()))
                .doOnNext(count -> {
                    if (count > 0) {
                        log.debug("Fired {} job(s)", count);
                    }
                });
    }

    private Mono<FiredJob> claimAndAdvance(Job job) {
        Instant scheduled = job.getNextFireAt();
        long fireEpoch = scheduled.getEpochSecond();
        Instant now = Instant.now();
        Instant next = ScheduleCalculator.nextAfterFire(job, scheduled, now);
        Mono<Void> advance = next == null ? store.complete(job.getId()) : store.reschedule(job.getId(), next);
        return store.tryClaim(job.getId(), fireEpoch, props.workerId())
                // A lost claim means a previous run already owns this (jobId, fireEpoch) —
                // advance the schedule anyway so the job never wedges. The claim-then-publish
                // gap left by a crash here is exactly what the M2 outbox closes.
                .flatMap(claimed -> advance.then(claimed
                        ? Mono.just(new FiredJob(job.getId(), job.getType(), job.getPayload(), fireEpoch, scheduled))
                        : Mono.empty()));
    }

    @Override
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
            subscription = null;
        }
    }

    @Override
    public boolean isRunning() {
        return subscription != null && !subscription.isDisposed();
    }
}
