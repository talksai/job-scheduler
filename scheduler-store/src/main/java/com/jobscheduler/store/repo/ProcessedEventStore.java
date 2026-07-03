package com.jobscheduler.store.repo;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/** Consumer-side dedup: the first insert of an event key wins, replays are no-ops. */
@Component
public class ProcessedEventStore {

    private final DatabaseClient db;

    public ProcessedEventStore(DatabaseClient db) {
        this.db = db;
    }

    /** @return true when this is the first time the event key is seen. */
    public Mono<Boolean> markProcessed(String eventKey) {
        return db.sql("INSERT INTO processed_event (event_key) VALUES (:eventKey) ON CONFLICT DO NOTHING")
                .bind("eventKey", eventKey)
                .fetch().rowsUpdated()
                .map(rows -> rows == 1);
    }
}
