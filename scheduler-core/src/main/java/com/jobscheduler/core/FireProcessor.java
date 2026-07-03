package com.jobscheduler.core;

import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.entity.Job;
import com.jobscheduler.store.repo.JobClaimStore;
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
 * execution table, advance the schedule — one transaction — then publish after
 * commit. Fires later than the missed-fire threshold go through the job's
 * MissedFirePolicy instead of the normal path.
 */
@Component
public class FireProcessor {

    private final JobClaimStore store;
    private final TransactionalOperator tx;
    private final FirePublisher publisher;
    private final SchedulerProperties props;
    private final SchedulerMetrics metrics;

    public FireProcessor(JobClaimStore store, TransactionalOperator tx, FirePublisher publisher,
                         SchedulerProperties props, SchedulerMetrics metrics) {
        this.store = store;
        this.tx = tx;
        this.publisher = publisher;
        this.props = props;
        this.metrics = metrics;
    }

    /** Fires whichever of the given jobs are still PENDING and due; returns the fire count. */
    public Mono<Integer> processDue(Collection<UUID> jobIds) {
        if (jobIds.isEmpty()) {
            return Mono.just(0);
        }
        return store.lockJobsForFire(jobIds)
                .concatMap(this::fireOne)
                .collectList()
                .as(tx::transactional)
                .flatMap(fired -> fired.isEmpty()
                        ? Mono.just(0)
                        : publisher.publishAll(fired).thenReturn(fired.size()));
    }

    private Flux<FiredJob> fireOne(Job job) {
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
                    // A lost claim means a previous run already owns this (jobId, fireEpoch) —
                    // skip the publish but still advance so the job never wedges. The
                    // claim-then-publish crash gap here is what the M2 outbox closes.
                    return store.tryClaim(job.getId(), fireEpoch, props.workerId())
                            .flatMap(claimed -> claimed
                                    ? Mono.just(new FiredJob(job.getId(), job.getType(), job.getPayload(),
                                            fireEpoch, occurrence, late))
                                    : Mono.empty());
                })
                .collectList()
                .flatMapMany(fired -> advance(job, next).thenMany(Flux.fromIterable(fired)));
    }

    private Mono<Void> advance(Job job, Instant next) {
        return next == null ? store.complete(job.getId()) : store.reschedule(job.getId(), next);
    }
}
