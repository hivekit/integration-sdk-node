# Hivekit Integration SDK for Node.js

Plain JavaScript, native `fetch`, and no external dependencies. Requires Node.js
22 or newer. Supports both ES modules and CommonJS:

```js
import { IntegrationClient } from '@hivekit/integration-sdk-node';
// Or: const { IntegrationClient } = require('@hivekit/integration-sdk-node');
```

## Use a previously registered job

Registration and execution can happen in separate processes. A runtime can look
up a job by its stable key without registering or changing its manifest.

```js
const client = new IntegrationClient({
    url: 'http://localhost:8090/v2/',
    token: process.env.HIVEKIT_TOKEN,
});

try {
    const job = await client.getJob('fleet-import');
    const execution = await job.startExecution([
        { key: 'fetch-data', name: 'Fetch fleet data', weight: 0.4 },
        { key: 'publish', name: 'Publish updates', weight: 0.6 },
    ]);

    const fetchStep = execution.startStep('fetch-data');
    const records = await fetchFleetData(execution.config);
    fetchStep.progress(0.5);
    fetchStep.info('Fetched fleet data', { count: records.length });
    await fetchStep.complete();

    const publishStep = execution.startStep('publish');
    await publishUpdates(records);
    await publishStep.complete();

    await execution.complete();
} finally {
    await client.close();
}
```

`fetchFleetData` and `publishUpdates` above are your own integration functions.
For Lambda handlers, construct the client inside the handler and await `close()`
in `finally` before returning. The SDK keeps reports in memory and writes no files.

## Jobs

```js
const job = await client.registerJob('fleet-import', {
    name: 'Fleet Import',
    kind: 'scheduled', // scheduled, continuous, manual
    expectedIntervalSeconds: 900,
    executionTimeoutSeconds: 300,
    configuration: {
        endpoint: { type: 'string', required: true },
        apiKey: { type: 'string', required: true, secret: true },
        enabled: { type: 'boolean', default: true },
    },
});

const existing = await client.getJob('fleet-import');
const jobs = await client.listJobs({ limit: 100, afterKey: 'fleet-import' });
await client.deregisterJob('fleet-import');
```

`registerJob(key, manifest)` creates or updates a manifest and supplies its `key`.
Manifest fields follow the server API, including the original configuration field
map or the supported JSON Schema object. Configuration values are managed through
the server API separately. Step definitions belong to `startExecution`, never the
manifest.

Jobs expose `id`, `key`, `manifest`, `manifestRevision`, `configRevision`, and
`monitoringEnabled`. `getJob` always queries the server and rejects with status
404 when absent. `listJobs` returns a page of job handles; filters are `key`,
`afterKey`, and `limit` (1–1000, default 100).

Deregistration permanently deletes the job, its configuration, and all associated
executions, steps and logs. The server retains audit entries. This requires the
new `DELETE service/integration/job/{jobId}` endpoint.

## Executions and steps

```js
const execution = await job.startExecution([
    { key: 'fetch-data', weight: 0.7 },
    { key: 'publish', weight: 0.3 },
], { metadata: { source: 'fleet-api' } });

const step = execution.startStep('fetch-data'); // Synchronous handle
step.progress(0.5);                           // Synchronously queued
step.debug('Starting request');
step.info('Received records', { count: 42 });
step.warn('Using cached data');
step.error('A recoverable error', { code: 'RETRY' });

await step.complete();
// Or: await step.fail('Upstream unavailable', { status: 503 });
// Or: await step.skip('Nothing to process');

await execution.startStep('publish').skip('No updates required');
await execution.complete();
// On failure: await execution.fail(errorOrReason, optionalDetails);
```

- `startExecution(steps = [], { metadata } = {})` declares up to 1000 unique steps.
  Each definition has a `key`, optional `name` (defaults to the key), and optional
  `weight` (defaults to 1). Positive weights up to 1 are relative; they needn't sum
  to 1. The server stores weights with six decimal places.
- `execution.config` is its resolved, frozen configuration snapshot. Secret values
  are available here. Later customer edits affect subsequent executions.
- Each invocation starts a new execution. Network retries reuse an internally
  generated request identity. There is no public request-key option or automatic
  rerun of integration code.
- Progress is a fraction from 0 to 1. Lower, delayed values are ignored. The SDK
  converts progress to the server's 0–100 representation.
- `startStep(key)` requires a declared key and queues its running state. Repeated
  starts of the same running step return the same handle.
- Completing or skipping a step sets progress to 1. Failing preserves the last
  progress. A failed step leaves the execution open for other steps or cleanup.
- A step exposes `key`, `name`, `weight`, `status`, and `progressValue`.
- `execution.complete()` requires all steps to be complete or skipped; it works
  with an empty step list too. `execution.fail(reason, details)` can end a run with
  unfinished steps. Both flush pending reports before reporting the outcome.
- `step.fail` and `execution.fail` also emit an error log. Reasons may be strings
  or `Error` objects; details must be JSON serializable (`Error` details include
  name, message, and stack).
- `execution.debug/info/warn/error` create execution-wide logs. The same methods
  on steps automatically associate logs with the step.

Automatic heartbeats start with each execution. The SDK selects an interval from
the manifest's `heartbeatTimeoutSeconds`, capped at ten seconds. Heartbeats stop
on execution completion or client close. Background timers are unreferenced so
they do not keep a process alive by themselves.

## Logging, batching, and shutdown

Every valid log call immediately calls Node's console: `debug`/`info` write to
stdout, `warn`/`error` to stderr, including the job, step, message and supplied
details. The same log is timestamped, snapshotted, and queued for HTTP delivery.
There are no severity filters and no disk spool or process-wide exit handlers.
Configuration and tokens are never automatically logged.

Pending reports flush every second, with log batches capped at 100 entries and
1 MiB. Step updates are ordered with logs; consecutive unsent running-progress
updates are coalesced. Later caller mutations cannot change queued log details.

Use `await execution.flush()` or `await client.flush()` for an explicit delivery
barrier. Await `step.complete()`/`fail()`/`skip()`, `execution.complete()`/`fail()`,
and `client.close()` to observe reporting errors. Background failures go to
`onError`, or stderr by default. Rejected batches stay in memory for a later flush;
an unsuccessful `close()` can be retried. A recovered log batch may be delivered
twice after an ambiguous connection failure, since the server cannot deduplicate
log batches.

`close()` waits for in-flight job operations, stops timers and flushes reports. It
does not mark running executions complete or failed. A closed client cannot be
reused; create another for the next Lambda invocation.

The queue is limited to 10,000 reports and 8 MiB to bound memory use. At capacity,
new reports throw instead of being silently dropped; log calls still make their
console copy first. Await `flush()` to release capacity. Individual messages must
be non-empty strings up to 16,384 UTF-8 bytes, and log entries must fit within a
1 MiB request. JSON numbers must be finite; unsafe JavaScript integers are rejected.

Console output provides a separate copy for the host's log capture. Fatal process
termination can lose pending HTTP batches; stdout/stderr capture follows the host
platform's behavior. Awaiting `close()` is required for ordinary invocation exit.

## Connection options and errors

```js
const client = new IntegrationClient({
    url: 'https://example.com/custom/v2/', // REST prefix, with or without trailing slash
    token: async () => obtainCurrentToken(),
    flushIntervalMs: 1000,               // Optional
    requestTimeoutMs: 10000,             // Optional, per HTTP attempt
    onError(error) {
        console.error('Integration reporting failed', error);
    },
});
```

Tokens may be strings or asynchronous providers. Requests always use bearer
headers and JSON content type, including GET. HTTP API errors reject with
`IntegrationApiError` (`status`, `message`); connection failures and timeouts use
`IntegrationTransportError` (`message`, `cause`).

Reads, execution starts, step reports, heartbeats and completion use bounded
transport retries. Manifest registration and deletion are not automatically
retried. Server authorization is unchanged: `C` registers, `R` looks up/lists,
`P` reports executions, and `D` deletes. An execution-only runtime using
`getJob(key)` typically needs `RP`; an operator looking up and deleting needs `RD`.

## Tests

```sh
npm test
```

Tests use Node's built-in test runner and localhost HTTP servers; no installation
or external dependencies are required. They cover background batching, console
copies before a forced process crash, retries, concurrent flushes, state
transitions, queue limits, heartbeat shutdown, and CommonJS/ES module imports.

An additional test uses a running Hivekit server when both
`HIVEKIT_INTEGRATION_TEST_URL` and `HIVEKIT_INTEGRATION_TEST_TOKEN` are set. It
creates and deletes only its randomly named job. The test server needs PostgreSQL,
integration encryption keys, the deletion endpoint, and credentials with
`CRUPD` permissions (or deployment administrator access).
