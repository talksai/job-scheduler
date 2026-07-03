package com.jobscheduler.core;

import com.jobscheduler.coordinator.CoordinationService;
import com.jobscheduler.coordinator.ShardOwnership;
import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.entity.Job;
import com.jobscheduler.store.repo.JobClaimStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * The fire path: lock the candidate rows, claim each due occurrence via the
 * execution table, advance the schedule, and durably enqueue the fire events
 * (transactional outbox) — ALL in one transaction, so there is no dual-write
 * window: either the claim, the schedule advance and the event all commit, or
 * none do. Fires later than the missed-fire threshold go through the job's
 * MissedFirePolicy instead of the normal path.
 */
@Component
public class FireProcessor {

    private static final Logger log = LoggerFactory.getLogger(FireProcessor.class);

    private final JobClaimStore store;
    private final TransactionalOperator tx;
    private final FirePublisher publisher;
    private final SchedulerProperties props;
    private final SchedulerMetrics metrics;
    private final ShardOwnership ownership;
    private final CoordinationService coordination;

    public FireProcessor(JobClaimStore store, TransactionalOperator tx, FirePublisher publisher,
                         SchedulerProperties props, SchedulerMetrics metrics,
                         ShardOwnership ownership, CoordinationService coordination) {
        this.store = store;
        this.tx = tx;
        this.publisher = publisher;
        this.props = props;
        this.metrics = metrics;
        this.ownership = ownership;
        this.coordination = coordination;
    }

    /** Fires whichever of the given jobs are still PENDING and due; returns the fire count. */
    public Mono<Integer> processDue(Collection<UUID> jobIds) {
        if (jobIds.isEmpty()) {
            return Mono.just(0);
        }
        return store.lockJobsForFire(jobIds)
                .concatMap(this::fireOne)
                .collectList()
                .flatMap(fired -> fired.isEmpty()
                        ? Mono.just(fired)
                        : publisher.enqueueAll(fired).thenReturn(fired))
                .as(tx::transactional)
                .flatMap(fired -> {
                    if (fired.isEmpty()) {
                        return Mono.just(0);
                    }
                    Instant firedAt = Instant.now();
                    fired.forEach(job -> metrics.jobFired(job.scheduledAt(), firedAt, job.late()));
                    return publisher.onCommitted(fired).thenReturn(fired.size());
                });
    }

    private Flux<FiredJob> fireOne(Job job) {
        // partition discipline: only the assigned owner fires a shard, and only
        // under a non-stale token. Skipped jobs stay PENDING for the real owner's
        // hydration; the execution-claim PK backstops any race regardless.
        if (!ownership.ownedShards().contains(job.getShard().intValue())
                || coordination.isFenced(ownership.epoch())) {
            log.debug("Skipping job {} on shard {} — not owned (epoch {})",
                    job.getId(), job.getShard(), ownership.epoch());
            return Flux.empty();
        }
        Instant now = Instant.now();
        Instant scheduled = job.getNextFireAt();
        boolean late = Duration.between(scheduled, now).toMillis() > props.missedFireThresholdMs();
        if (!late) {
            return claimOccurrences(job, List.of(scheduled), false,
                    ScheduleCalculator.nextFutureOccurrence(job, scheduled, now));
        }
        return switch (job.getMissedFirePolicy()) {
            case FIRE_ONCE_IMMEDIATELY -> claimOccurrences(job, List.of(scheduled), true,
                    ScheduleCalculator.nextFutureOccurrence(job, scheduled, now));
            case SKIP -> {
                metrics.fireSkipped();
                yield advance(job, ScheduleCalculator.nextFutureOccurrence(job, scheduled, now))
                        .thenMany(Flux.empty());
            }
            case FIRE_ALL -> {
                List<Instant> occurrences =
                        ScheduleCalculator.occurrencesThrough(job, scheduled, now, props.maxCatchupFires());
                // may still be in the past when the cap truncated the batch — the next
                // hydration re-picks the job and catch-up resumes from there
                Instant next = ScheduleCalculator.occurrenceAfter(job, occurrences.getLast());
                yield claimOccurrences(job, occurrences, true, next);
            }
        };
    }

    /** Claims each occurrence, then advances the schedule; runs inside the caller's tx. */
    private Flux<FiredJob> claimOccurrences(Job job, List<Instant> occurrences, boolean late, Instant next) {
        return Flux.fromIterable(occurrences)
                .concatMap(occurrence -> {
                    long fireEpoch = occurrence.getEpochSecond();
                    // A lost claim means the (jobId, fireEpoch) is already FIRED or held
                    // under a live lease — skip the publish but still advance so the job
                    // never wedges.
                    return store.tryClaim(job.getId(), fireEpoch, props.workerId(),
                                    props.claimLeaseSeconds(), ownership.epoch())
                            .flatMap(claimed -> claimed
                                    ? Mono.just(new FiredJob(job.getId(), job.getType(), job.getPayload(),
                                            fireEpoch, occurrence, late, newTraceId()))
                                    : Mono.empty());
                })
                .collectList()
                .flatMapMany(fired -> advance(job, next).thenMany(Flux.fromIterable(fired)));
    }

    private Mono<Void> advance(Job job, Instant next) {
        return next == null ? store.complete(job.getId()) : store.reschedule(job.getId(), next);
    }

    private static String newTraceId() {
        UUID id = UUID.randomUUID();
        return Long.toHexString(id.getMostSignificantBits()) + Long.toHexString(id.getLeastSignificantBits());
    }
}
