// k6 load profile: create one-shot jobs at a constant arrival rate, each firing
// 5-30s later — so job creation AND firing run at ~RATE/s sustained.
// RATE=100/s ≈ 8.6M jobs/day equivalent.
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    create_jobs: {
      executor: 'constant-arrival-rate',
      rate: parseInt(__ENV.RATE || '100'),
      timeUnit: '1s',
      duration: __ENV.DURATION || '3m',
      preAllocatedVUs: 50,
      maxVUs: 300,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    http_req_duration: ['p(99)<500'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://app:8080';

export default function () {
  const fireInMs = 5000 + Math.floor(Math.random() * 25000);
  const res = http.post(`${BASE_URL}/api/jobs`, JSON.stringify({
    type: 'bench',
    payload: '{"n":1}',
    scheduleType: 'ONE_SHOT',
    fireAt: new Date(Date.now() + fireInMs).toISOString(),
  }), { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'created 201': (r) => r.status === 201 });
}
