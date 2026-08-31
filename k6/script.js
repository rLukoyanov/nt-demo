import http from 'k6/http';
import { check } from 'k6';

// Step-like profile: 10s static 300 RPS, 8s peak 1500 RPS, repeat.
export const options = {
  discardResponseBodies: true,
  scenarios: {
    peaks: {
      executor: 'ramping-arrival-rate',
      startRate: 300,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
      stages: [
        { target: 300, duration: '10s' },   // static 300
        { target: 1500, duration: '1s' },   // spike to 1500
        { target: 1500, duration: '7s' },   // hold 1500
        { target: 300, duration: '1s' },    // drop to 300
        { target: 300, duration: '9s' },    // static 300
        { target: 1500, duration: '1s' },   // spike again
        { target: 1500, duration: '7s' },
        { target: 300, duration: '1s' },
        { target: 300, duration: '5s' },
      ],
    },
  },
};

export default function () {
  const res = http.get('http://target.default.svc.cluster.local:8080/');
  check(res, { 'status is ok': (r) => r.status === 200 || r.status === 404 });
}