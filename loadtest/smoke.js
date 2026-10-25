import http from 'k6/http';

// Smoke test: single request through the full containerized chain
// (k6 -> envoy -> extproc (gRPC) -> echo upstream).
//
// Target: TEST_TARGET (default http://localhost:8080, i.e. the envoy listener
// published by tests/compose.yaml).
// Run: just loadtest-smoke

const baseUrl = __ENV.TEST_TARGET || 'http://localhost:8080';

export const options = {
  scenarios: {
    smoke: {
      executor: 'per-vu-iterations',
      vus: 1,
      iterations: 1,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
  },
};

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }));

export default function () {
  http.post(`${baseUrl}/`, JSON.stringify({}), {
    headers: {
      'Content-Type': 'application/json',
    },
  });
}
