'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { IntegrationClient, IntegrationApiError } = require('..');

const url = process.env.HIVEKIT_INTEGRATION_TEST_URL;
const token = process.env.HIVEKIT_INTEGRATION_TEST_TOKEN;

test('reports real integration lifecycles and deregistration against the Go server', { skip: !url || !token }, async () => {
    const key = `sdk-test-${randomUUID()}`;
    const client = new IntegrationClient({ url, token });
    const base = `${url.replace(/\/+$/, '')}/service/integration/`;
    const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
    let job;
    let deleted = false;
    try {
        job = await client.registerJob(key, {
            name: 'SDK HTTP test', kind: 'manual',
            configuration: { endpoint: { type: 'string', default: 'test-endpoint' }, password: { type: 'string', secret: true } }
        });
        const configured = await fetch(`${base}job/${job.id}/configuration`, {
            method: 'PUT', headers, body: JSON.stringify({ values: { password: 'test-only-credential' } })
        });
        assert.equal(configured.status, 200);

        const existing = await client.getJob(key);
        assert.equal(existing.id, job.id);
        assert.equal((await client.listJobs({ key }))[0].id, job.id);
        const execution = await existing.startExecution([{ key: 'fetch', weight: 0.7 }, { key: 'publish', weight: 0.3 }], { metadata: { source: 'sdk' } });
        assert.deepEqual(execution.config, { endpoint: 'test-endpoint', password: 'test-only-credential' });
        const step = execution.startStep('fetch');
        step.progress(0.5);
        step.debug('Fetching');
        step.info('Fetched records', { count: 2 });
        step.warn('Using a test endpoint');
        await execution.startStep('publish').skip('No updates required');
        const observation = await (await fetch(`${base}execution/${execution.id}`, { headers })).json();
        assert.equal(observation.data.progress, 65);
        await step.complete();
        await execution.complete();
        const logs = await (await fetch(`${base}execution/${execution.id}/log/`, { headers })).json();
        assert.ok(logs.data.some(log => log.message === 'Fetched records' && log.stepKey === 'fetch' && log.details.count === 2));

        const failed = await existing.startExecution([{ key: 'fetch' }]);
        const failedStep = failed.startStep('fetch');
        failedStep.progress(0.25);
        await failedStep.fail(new Error('Test upstream failure'), { code: 'TEST' });
        assert.equal(failedStep.progressValue, 0.25);
        await failed.fail('Test execution failed');
        const failure = await (await fetch(`${base}execution/${failed.id}`, { headers })).json();
        assert.equal(failure.data.status, 'failed');
        assert.equal(failure.data.progress, 25);

        await client.deregisterJob(key);
        deleted = true;
        await assert.rejects(client.getJob(key), error => error instanceof IntegrationApiError && error.status === 404);
        assert.equal((await fetch(`${base}execution/${execution.id}`, { headers })).status, 404);
    } finally {
        await client.close();
        if (job && !deleted) await fetch(`${base}job/${job.id}`, { method: 'DELETE', headers });
    }
});
