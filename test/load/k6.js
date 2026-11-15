// k6 load test for the job API.
//   k6 run -e API_URL=http://localhost:8080 -e API_KEY=$JOBCTL_API_KEY test/load/k6.js
// Tunables: RATE (jobs/s, default 200), DURATION (default 2m), JOB_TYPE (default noop).
// It submits at a fixed arrival rate (open model: slow responses do not reduce the offered
// load, which is how real traffic behaves) and, in a second scenario, polls a sample of jobs
// to measure end-to-end completion latency.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const API = __ENV.API_URL || 'http://localhost:8080';
const KEY = __ENV.API_KEY;
const RATE = parseInt(__ENV.RATE || '200');
const DURATION = __ENV.DURATION || '2m';
const JOB_TYPE = __ENV.JOB_TYPE || 'noop';

const e2e = new Trend('job_end_to_end_ms', true);
const completed = new Rate('jobs_completed');

export const options = {
  scenarios: {
    submit: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 50, maxVUs: 400,
      exec: 'submit',
    },
    // Sample 1 job per second end to end: submit, then poll until it finishes.
    e2e: {
      executor: 'constant-arrival-rate',
      rate: 1, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 30, maxVUs: 100,
      exec: 'endToEnd',
    },
  },
  thresholds: {
    // Submitting must stay fast and reliable even under load.
    'http_req_duration{scenario:submit}': ['p(95)<250', 'p(99)<750'],
    'http_req_failed{scenario:submit}': ['rate<0.01'],
    // Workers must keep up: most sampled jobs finish within 30s (the pickup SLO).
    jobs_completed: ['rate>0.99'],
    job_end_to_end_ms: ['p(95)<30000'],
  },
};

const headers = { Authorization: `Bearer ${KEY}`, 'Content-Type': 'application/json' };

export function setup() {
  if (!KEY) throw new Error('set -e API_KEY=<tenant api key>');
}

export function submit() {
  const res = http.post(`${API}/jobs`, JSON.stringify({ type: JOB_TYPE, payload: { n: __ITER } }), { headers });
  check(res, { 'created (201)': (r) => r.status === 201 });
}

export function endToEnd() {
  const t0 = Date.now();
  const res = http.post(`${API}/jobs`, JSON.stringify({ type: JOB_TYPE }), { headers });
  if (res.status !== 201) { completed.add(false); return; }
  const id = res.json('id');
  for (let i = 0; i < 120; i++) {
    const st = http.get(`${API}/jobs/${id}`, { headers, tags: { name: 'GET /jobs/{id}' } }).json('status');
    if (st === 'succeeded') { e2e.add(Date.now() - t0); completed.add(true); return; }
    if (st === 'dead' || st === 'failed') { completed.add(false); return; }
    sleep(0.5);
  }
  completed.add(false);
}
