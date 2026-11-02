'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createServer } = require('node:http');
const { randomUUID } = require('node:crypto');
const { spawn } = require('node:child_process');
const { IntegrationClient, IntegrationApiError, IntegrationTransportError } = require('..');

const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

async function until(check) {
    const deadline = Date.now() + 3000;
    while (!check()) {
        if (Date.now() > deadline) throw new Error('Timed out waiting for a background report');
        await pause(10);
    }
}

// A real HTTP boundary exercises native fetch, headers, JSON envelopes and
// server state transitions. It implements only the integration wire contract.
async function fixture(t, options = {}) {
    const requests = [];
    const jobs = new Map();
    const executions = new Map();
    const starts = new Map();
    const state = { jobs, executions, requests };
    const server = createServer(async (req, res) => {
        try {
            const url = new URL(req.url, 'http://localhost');
            const chunks = [];
            for await (const chunk of req) chunks.push(chunk);
            const text = Buffer.concat(chunks).toString();
            const request = { method: req.method, path: url.pathname, query: url.searchParams, headers: req.headers, body: text ? JSON.parse(text) : undefined };
            requests.push(request);
            let result = options.handle && await options.handle(request, state);
            if (!result) {
                const path = decodeURIComponent(url.pathname.replace('/custom/v2/service/integration/', ''));
                const id = path.split('/')[1];
                if (path === 'job/' && req.method === 'POST') {
                    const manifest = request.body.manifest;
                    const job = jobs.get(manifest.key) || { id: randomUUID(), key: manifest.key, manifestRevision: 1, configRevision: 1, monitoringEnabled: true, configuration: { endpoint: 'original' } };
                    job.manifest = manifest;
                    jobs.set(job.key, job);
                    result = { data: job };
                } else if (path === 'job/' && req.method === 'GET') {
                    const records = [...jobs.values()].filter(job => (!url.searchParams.get('key') || job.key === url.searchParams.get('key')) && job.key > (url.searchParams.get('afterKey') || '')).sort((a, b) => a.key.localeCompare(b.key));
                    result = { data: records.slice(0, Number(url.searchParams.get('limit') || 100)) };
                } else if (path.startsWith('job/') && req.method === 'DELETE') {
                    const job = [...jobs.values()].find(job => job.id === id);
                    if (!job) result = { status: 404, message: 'Integration resource not found' };
                    else {
                        jobs.delete(job.key);
                        for (const [key, execution] of executions) if (execution.jobId === job.id) executions.delete(key);
                        result = { data: { id } };
                    }
                } else if (path.startsWith('job/') && path.endsWith('/execution/') && req.method === 'POST') {
                    const job = [...jobs.values()].find(job => job.id === id);
                    const requestKey = `${id}:${request.body.idempotencyKey}`;
                    let execution = starts.get(requestKey);
                    if (!execution) {
                        execution = { id: randomUUID(), jobId: id, status: 'running', manifestRevision: job.manifestRevision, configRevision: job.configRevision,
                            steps: new Map(request.body.steps.map(step => [step.key, { ...step, status: 'pending', progress: 0 }])), logs: [], heartbeats: 0,
                            configuration: structuredClone(job.configuration), idempotencyKey: request.body.idempotencyKey };
                        executions.set(execution.id, execution);
                        starts.set(requestKey, execution);
                    }
                    result = { data: { execution: { ...execution, steps: undefined }, configuration: execution.configuration } };
                } else if (path.startsWith('execution/')) {
                    const execution = executions.get(id);
                    if (!execution) result = { status: 404, message: 'Integration resource not found' };
                    else if (path.includes('/step/')) {
                        const step = execution.steps.get(path.split('/step/')[1]);
                        if (execution.status !== 'running') result = { status: 409, message: 'Execution is complete' };
                        else {
                            Object.assign(step, request.body);
                            result = { data: step };
                        }
                    } else if (path.endsWith('/log/')) {
                        execution.logs.push(...request.body.logs);
                        result = { raw: { res: 'suc' } };
                    } else if (path.endsWith('/heartbeat')) {
                        execution.heartbeats++;
                        result = { data: execution };
                    } else if (path.endsWith('/complete')) {
                        if (request.body.status === 'succeeded' && [...execution.steps.values()].some(step => !['succeeded', 'skipped'].includes(step.status))) result = { status: 409, message: 'Unfinished steps' };
                        else {
                            execution.status = request.body.status;
                            result = { data: execution };
                        }
                    }
                }
            }
            result ||= { status: 404, message: 'Unknown route' };
            if (options.after && await options.after(request, result, state) === 'disconnect') {
                res.destroy();
                return;
            }
            res.writeHead(result.status || 200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(result.raw || (result.message ? { result: 'error', message: result.message } : { result: 'success', data: result.data })));
        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ result: 'error', message: error.message }));
        }
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const url = `http://127.0.0.1:${server.address().port}/custom/v2/`;
    const errors = [];
    const client = new IntegrationClient({ url, token: 'test-token', flushIntervalMs: 60000, onError: error => errors.push(error), ...options.client });
    t.after(async () => {
        await client.close().catch(() => {});
        server.closeAllConnections();
        await new Promise(resolve => server.close(resolve));
    });
    for (const level of ['debug', 'info', 'warn', 'error']) t.mock.method(console, level, () => {});
    return { ...state, client, url, errors };
}

test('CommonJS and ESM exports use the same dependency-free client', async () => {
    const esm = await import('../index.mjs');
    assert.equal(esm.IntegrationClient, IntegrationClient);
    assert.equal(esm.IntegrationApiError, IntegrationApiError);
});

test('registers, resolves pre-existing jobs, lists and deregisters by stable key', async t => {
    const { client, jobs, requests } = await fixture(t);
    const a = await client.registerJob('a/key', { name: 'A', kind: 'manual' });
    await client.registerJob('b', { name: 'B', kind: 'manual' });
    const fetched = await client.getJob('a/key');
    assert.equal(fetched.id, a.id);
    assert.deepEqual((await client.listJobs({ limit: 1, afterKey: 'a/key' })).map(job => job.key), ['b']);
    await client.deregisterJob('a/key');
    assert.equal(jobs.has('a/key'), false);
    await assert.rejects(client.getJob('a/key'), error => error instanceof IntegrationApiError && error.status === 404);
    for (const request of requests) {
        assert.equal(request.headers.authorization, 'Bearer test-token');
        assert.equal(request.headers['content-type'], 'application/json');
        assert.equal(request.query.has('token'), false);
    }
});

test('uses an existing job without ever registering it and supports refreshed tokens', async t => {
    let tokenNumber = 0;
    const { client, jobs, requests } = await fixture(t, { client: { token: async () => `token-${++tokenNumber}` } });
    jobs.set('existing', { id: randomUUID(), key: 'existing', manifest: { kind: 'manual' }, configuration: { password: 'secret' }, manifestRevision: 2, configRevision: 3 });
    const job = await client.getJob('existing');
    const execution = await job.startExecution();
    assert.equal(execution.config.password, 'secret');
    assert.equal(execution.configRevision, 3);
    assert.equal(Object.hasOwn(execution, 'idempotencyKey'), false);
    assert.deepEqual(requests.map(request => request.method), ['GET', 'POST']);
    assert.deepEqual(requests.map(request => request.headers.authorization), ['Bearer token-1', 'Bearer token-2']);
    await execution.complete();
});

test('mirrors all log levels immediately, snapshots details and batches HTTP delivery', async t => {
    const { client, requests, executions } = await fixture(t);
    const job = await client.registerJob('logs', { name: 'Logs', kind: 'manual' });
    const execution = await job.startExecution([{ key: 'fetch', weight: 0.5 }]);
    const step = execution.startStep('fetch');
    const details = { count: 3 };
    for (const level of ['debug', 'info', 'warn', 'error']) {
        step[level](`${level} message`, details);
        assert.equal(console[level].mock.calls.length, 1);
        assert.match(console[level].mock.calls[0].arguments[0], /\[logs\]\[fetch\]/);
    }
    assert.equal(requests.some(request => request.path.endsWith('/log/')), false);
    details.count = 99;
    await step.complete();
    const batches = requests.filter(request => request.path.endsWith('/log/'));
    assert.equal(batches.length, 1);
    assert.deepEqual(batches[0].body.logs.map(log => log.level), ['debug', 'info', 'warn', 'error']);
    assert.ok(batches[0].body.logs.every(log => log.stepKey === 'fetch' && log.details.count === 3 && log.occurredAt));
    assert.equal(executions.get(execution.id).steps.get('fetch').progress, 100);
    await execution.complete();
});

test('coalesces fractional progress, ignores regressions, preserves failed progress and keeps other steps open', async t => {
    const { client, requests, executions } = await fixture(t);
    const job = await client.registerJob('progress', { name: 'Progress', kind: 'manual' });
    const execution = await job.startExecution([{ key: 'fetch', weight: 0.4 }, { key: 'cleanup', weight: 0.6 }]);
    const step = execution.startStep('fetch');
    step.progress(0.25);
    step.progress(0.5);
    step.progress(0.1);
    assert.throws(() => step.progress(1.1), /between 0 and 1/);
    assert.throws(() => step.progress(NaN), /between 0 and 1/);
    await step.fail('Upstream unavailable', { retry: true });
    assert.equal(step.progressValue, 0.5);
    assert.equal(step.status, 'failed');
    assert.equal(execution.status, 'running');
    const updates = requests.filter(request => request.path.endsWith('/step/fetch'));
    assert.deepEqual(updates.map(request => request.body), [{ status: 'running', progress: 50 }, { status: 'failed', progress: 50 }]);
    const cleanup = execution.startStep('cleanup');
    await cleanup.skip('Nothing to clean');
    await assert.rejects(execution.complete(), /Every step/);
    await execution.fail('Import failed');
    assert.equal(executions.get(execution.id).status, 'failed');
    assert.throws(() => step.progress(0.8), /already complete/);
});

test('flushes queued steps and logs before completing an execution, including unawaited step completion', async t => {
    const { client, executions } = await fixture(t);
    const job = await client.registerJob('finish', { name: 'Finish', kind: 'manual' });
    const execution = await job.startExecution([{ key: 'work' }]);
    const step = execution.startStep('work');
    step.info('Finished');
    const stepCompletion = step.complete();
    await execution.complete();
    await stepCompletion;
    assert.equal(executions.get(execution.id).logs.length, 1);
    assert.equal(executions.get(execution.id).status, 'succeeded');
    await execution.complete();
    await assert.rejects(execution.fail('Different outcome'), /different status/);
});

test('reports in the background and flushes at the batch limit', async t => {
    const { client, requests, executions } = await fixture(t, { client: { flushIntervalMs: 20 } });
    const job = await client.registerJob('background', { name: 'Background', kind: 'manual' });
    const execution = await job.startExecution();
    execution.info('Timer flush');
    await until(() => executions.get(execution.id).logs.length === 1);
    for (let i = 0; i < 201; i++) execution.info(`Message ${i}`);
    await client.flush();
    const batches = requests.filter(request => request.path.endsWith('/log/'));
    assert.ok(batches.every(request => request.body.logs.length <= 100));
    assert.equal(executions.get(execution.id).logs.length, 202);
    await execution.complete();
});

test('starts automatic heartbeats and stops them on completion', async t => {
    const { client, executions } = await fixture(t);
    const job = await client.registerJob('continuous', { name: 'Continuous', kind: 'continuous', heartbeatTimeoutSeconds: 1 });
    const execution = await job.startExecution();
    await until(() => executions.get(execution.id).heartbeats > 0);
    await execution.complete();
    const count = executions.get(execution.id).heartbeats;
    await pause(400);
    assert.equal(executions.get(execution.id).heartbeats, count);
});

test('retries an ambiguous start internally with the same request identity and snapshot', async t => {
    let disconnected = false;
    const { client, requests, executions } = await fixture(t, { after(request) {
        if (request.path.endsWith('/execution/') && !disconnected) {
            disconnected = true;
            return 'disconnect';
        }
    } });
    const job = await client.registerJob('retry', { name: 'Retry', kind: 'manual' });
    const execution = await job.startExecution([{ key: 'fetch', weight: 0.5 }], { metadata: { batch: 1 } });
    const starts = requests.filter(request => request.path.endsWith('/execution/'));
    assert.equal(starts.length, 2);
    assert.deepEqual(starts[0].body, starts[1].body);
    assert.equal(executions.size, 1);
    await execution.fail('Stopped');
    const second = await job.startExecution();
    assert.notEqual(second.id, execution.id);
    await second.complete();
});

test('does not lose progress queued while an earlier update is in flight', async t => {
    let release;
    let entered = false;
    const gate = new Promise(resolve => { release = resolve; });
    const { client, executions } = await fixture(t, { async handle(request) {
        if (request.path.includes('/step/') && !entered) {
            entered = true;
            await gate;
        }
    } });
    const job = await client.registerJob('inflight', { name: 'Inflight', kind: 'manual' });
    const execution = await job.startExecution([{ key: 'fetch' }]);
    const step = execution.startStep('fetch');
    step.progress(0.2);
    const flush = execution.flush();
    await until(() => entered);
    step.progress(0.8);
    release();
    await flush;
    assert.equal(executions.get(execution.id).steps.get('fetch').progress, 80);
    await step.complete();
    await execution.complete();
});

test('keeps rejected log batches in memory, exposes failures, and recovers on explicit flush', async t => {
    let reject = true;
    const { client, executions } = await fixture(t, { handle(request) {
        if (reject && request.path.endsWith('/log/')) return { status: 503, message: 'Unavailable' };
    } });
    const job = await client.registerJob('outage', { name: 'Outage', kind: 'manual' });
    const execution = await job.startExecution();
    execution.error('Keep this message');
    await assert.rejects(client.close(), error => error instanceof IntegrationApiError && error.status === 503);
    reject = false;
    await client.close();
    assert.equal(executions.get(execution.id).logs[0].message, 'Keep this message');
    assert.equal(executions.get(execution.id).status, 'running');
    assert.throws(() => execution.info('Closed'), /closed/);
});

test('surfaces background delivery errors without unhandled promise rejections', async t => {
    let reject = true;
    const { client, errors, executions } = await fixture(t, { client: { flushIntervalMs: 10 }, handle(request) {
        if (reject && request.path.endsWith('/log/')) return { status: 403, message: 'Permission denied' };
    } });
    const job = await client.registerJob('errors', { name: 'Errors', kind: 'manual' });
    const execution = await job.startExecution();
    execution.warn('Queued warning');
    await until(() => errors.length === 1);
    await pause(40);
    assert.equal(errors.length, 1);
    reject = false;
    await client.close();
    assert.equal(executions.get(execution.id).logs.length, 1);
});

test('close waits for already-starting executions and execution completion', async t => {
    const { client, executions } = await fixture(t);
    const job = await client.registerJob('closing', { name: 'Closing', kind: 'manual' });
    const starting = job.startExecution();
    await client.close();
    const execution = await starting;
    assert.equal(executions.size, 1);
    assert.throws(() => execution.info('Closed'), /closed/);

    const second = await fixture(t);
    const other = await (await second.client.registerJob('finish-closing', { name: 'Closing', kind: 'manual' })).startExecution();
    const finishing = other.fail('Intentional failure');
    await second.client.close();
    await finishing;
    assert.equal(second.executions.get(other.id).status, 'failed');
});

test('accepts diagnostic logs after completion and flushes them on close', async t => {
    const { client, executions } = await fixture(t);
    const execution = await (await client.registerJob('late', { name: 'Late', kind: 'manual' })).startExecution();
    await execution.complete();
    execution.info('Final diagnostic');
    await client.close();
    assert.equal(executions.get(execution.id).logs.length, 1);
});

test('request timeouts abort native fetch and return a transport error', async t => {
    const { client } = await fixture(t, { client: { requestTimeoutMs: 20 }, async handle() {
        await pause(100);
    } });
    await assert.rejects(client.registerJob('timeout', { name: 'Timeout', kind: 'manual' }), error => error instanceof IntegrationTransportError && /timed out/.test(error.message));
});

test('rejected async error handlers and invalid bearer tokens do not leak credentials or create unhandled rejections', async t => {
    let reject = true;
    const { client } = await fixture(t, { client: { flushIntervalMs: 10, onError: async () => { throw new Error('Handler failed'); } }, handle(request) {
        if (reject && request.path.endsWith('/log/')) return { status: 403, message: 'Denied' };
    } });
    const execution = await (await client.registerJob('handler', { name: 'Handler', kind: 'manual' })).startExecution();
    execution.info('Queued');
    await until(() => console.error.mock.calls.length > 0);
    reject = false;
    await client.close();
    assert.throws(() => new IntegrationClient({ url: 'http://localhost', token: 'private-token\n' }), error => !error.message.includes('private-token'));
});

test('rejects invalid public input before starting work', async t => {
    assert.throws(() => new IntegrationClient({ url: 'file:///tmp/test', token: 'secret' }), /HTTP/);
    assert.throws(() => new IntegrationClient({ url: 'http://localhost', token: 'secret', flushIntervalMs: 0 }), /positive/);
    const { client, requests } = await fixture(t);
    await assert.rejects(client.registerJob('a', { key: 'b' }), /must match/);
    await assert.rejects(client.registerJob('a', { steps: [] }), /startExecution/);
    await assert.rejects(client.listJobs({ limit: 0 }), /limit/);
    const job = await client.registerJob('validation', { name: 'Validation', kind: 'manual' });
    for (const steps of [[{ key: 'x', weight: 0 }], [{ key: 'x', weight: 2 }], [{ key: 'x' }, { key: 'x' }]]) {
        await assert.rejects(job.startExecution(steps));
    }
    await assert.rejects(job.startExecution([], { idempotencyKey: 'public-key' }), /Unknown execution option/);
    assert.equal(requests.filter(request => request.path.endsWith('/execution/')).length, 0);
    const execution = await job.startExecution();
    assert.throws(() => execution.startStep('unknown'), /not declared/);
    await execution.complete();
});

test('bounds the in-memory queue without silently dropping logs or their console copies', async t => {
    const { client, executions } = await fixture(t);
    const execution = await (await client.registerJob('bounded', { name: 'Bounded', kind: 'manual' })).startExecution();
    const details = { large: 'x'.repeat(900000) };
    let accepted = 0;
    assert.throws(() => {
        while (accepted < 20) {
            execution.info(`Entry ${accepted}`, details);
            accepted++;
        }
    }, /queue is full/);
    assert.ok(accepted > 0 && accepted < 20);
    assert.equal(console.info.mock.calls.length, accepted + 1);
    await client.close();
    assert.equal(executions.get(execution.id).logs.length, accepted);
});

test('writes real stdout/stderr before a forced crash without waiting for an HTTP batch', async t => {
    const { url, requests, jobs } = await fixture(t);
    jobs.set('crash', { id: randomUUID(), key: 'crash', manifest: { kind: 'manual' }, configuration: {}, manifestRevision: 1, configRevision: 1 });
    const child = spawn(process.execPath, ['--input-type=module', '-e', `
        import { IntegrationClient } from ${JSON.stringify(new URL('../index.mjs', `file://${__filename}`).href)};
        const client = new IntegrationClient({ url: ${JSON.stringify(url)}, token: 'test-token', flushIntervalMs: 60000 });
        const execution = await (await client.getJob('crash')).startExecution([{ key: 'work' }]);
        const step = execution.startStep('work');
        step.info('immediate stdout', { count: 1 });
        step.error('immediate stderr', { fatal: true });
        setInterval(() => {}, 1000);
    `], { stdio: ['ignore', 'pipe', 'pipe'] });
    t.after(() => child.kill('SIGKILL'));
    let stdout = '', stderr = '';
    child.stdout.on('data', data => { stdout += data; });
    child.stderr.on('data', data => { stderr += data; });
    await until(() => stdout.includes('immediate stdout') && stderr.includes('immediate stderr'));
    const exited = new Promise(resolve => child.once('exit', resolve));
    child.kill('SIGKILL');
    await exited;
    assert.equal(requests.filter(request => request.path.endsWith('/log/')).length, 0);
    assert.match(stdout, /count: 1/);
    assert.match(stderr, /fatal: true/);
});
