-- M2: claims carry a lease so a crashed worker's claim becomes re-claimable
-- after expiry instead of wedging the (job_id, fire_epoch) forever.
ALTER TABLE execution ADD COLUMN lease_until TIMESTAMPTZ;
