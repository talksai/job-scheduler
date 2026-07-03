package com.jobscheduler.api.web;

import com.jobscheduler.api.service.JobService;
import com.jobscheduler.api.web.dto.CreateJobRequest;
import com.jobscheduler.api.web.dto.JobResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<JobResponse> create(@Valid @RequestBody CreateJobRequest request) {
        return jobService.create(request).map(JobResponse::from);
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<JobResponse>> get(@PathVariable UUID id) {
        return jobService.get(id)
                .map(job -> ResponseEntity.ok(JobResponse.from(job)))
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<Void>> cancel(@PathVariable UUID id) {
        return jobService.cancel(id)
                .map(cancelled -> cancelled
                        ? ResponseEntity.noContent().<Void>build()
                        : ResponseEntity.notFound().<Void>build());
    }
}
