package com.jobscheduler.store.repo;

/** An unpublished outbox row ready for the poller. */
public record OutboxMessage(long id, String topic, String messageKey, String payload) {
}
