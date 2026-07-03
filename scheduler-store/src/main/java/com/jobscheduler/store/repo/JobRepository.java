package com.jobscheduler.store.repo;

import com.jobscheduler.store.entity.Job;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface JobRepository extends ReactiveCrudRepository<Job, UUID> {
}
