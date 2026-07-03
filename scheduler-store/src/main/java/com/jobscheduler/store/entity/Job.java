package com.jobscheduler.store.entity;

import com.jobscheduler.store.domain.JobState;
import com.jobscheduler.store.domain.MissedFirePolicy;
import com.jobscheduler.store.domain.ScheduleType;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Table("job")
public class Job {

    @Id
    private UUID id;
    private String type;
    private String payload;
    @Column("schedule_type")
    private ScheduleType scheduleType;
    @Column("cron_expression")
    private String cronExpression;
    @Column("fixed_rate_seconds")
    private Long fixedRateSeconds;
    @Column("fire_at")
    private Instant fireAt;
    @Column("missed_fire_policy")
    private MissedFirePolicy missedFirePolicy;
    private Short shard;
    @Column("next_fire_at")
    private Instant nextFireAt;
    private JobState state;
    private Long version;
    @Column("created_at")
    private Instant createdAt;

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public ScheduleType getScheduleType() { return scheduleType; }
    public void setScheduleType(ScheduleType scheduleType) { this.scheduleType = scheduleType; }
    public String getCronExpression() { return cronExpression; }
    public void setCronExpression(String cronExpression) { this.cronExpression = cronExpression; }
    public Long getFixedRateSeconds() { return fixedRateSeconds; }
    public void setFixedRateSeconds(Long fixedRateSeconds) { this.fixedRateSeconds = fixedRateSeconds; }
    public Instant getFireAt() { return fireAt; }
    public void setFireAt(Instant fireAt) { this.fireAt = fireAt; }
    public MissedFirePolicy getMissedFirePolicy() { return missedFirePolicy; }
    public void setMissedFirePolicy(MissedFirePolicy missedFirePolicy) { this.missedFirePolicy = missedFirePolicy; }
    public Short getShard() { return shard; }
    public void setShard(Short shard) { this.shard = shard; }
    public Instant getNextFireAt() { return nextFireAt; }
    public void setNextFireAt(Instant nextFireAt) { this.nextFireAt = nextFireAt; }
    public JobState getState() { return state; }
    public void setState(JobState state) { this.state = state; }
    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
