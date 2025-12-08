import http from 'k6/http';
import { check } from 'k6';

const VUS = __ENV.VUS || 10;
const DURATION = __ENV.DURATION || '10s';

// Optional: define VUs & duration
export const options = {
  vus: Number(VUS),
  duration: String(DURATION),
};

export default function () {
  // Generate UUID using JS (client-side, no shell call)
  const uuid = crypto.randomUUID();

  const url = `http://127.0.0.1:8080/ticket/ack/${uuid}`;
  const res = http.post(url, null, {});

  check(res, {
    'status is 202': (r) => r.status === 202,
  });
}

