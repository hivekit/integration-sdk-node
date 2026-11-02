'use strict';

const { randomUUID } = require('node:crypto');
const { IntegrationApiError, IntegrationTransportError } = require('./errors');
const { nonempty, object, snapshot, stepDefinitions } = require('./values');
const { Execution } = require('./execution');

function tokenValue(value) {
    nonempty(value, 'token');
    if (/[^\x21-\x7e]/.test(value)) throw new TypeError('token must be a raw ASCII bearer token without whitespace');
    return value;
}

class IntegrationClient {
    #baseUrl;
    #token;
    #timeout;
    #onError;
    #jobs = new Map();
    #executions = new Set();
    #operations = new Set();
    #closing;
    #closed = false;

    constructor({ url, token, flushIntervalMs = 1000, requestTimeoutMs = 10000, onError } = {}) {
        const base = new URL(nonempty(url, 'url'));
        if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash) {
            throw new TypeError('url must be an HTTP(S) REST base URL without credentials, query or fragment');
        }
        if (typeof token !== 'function') tokenValue(token);
        for (const [name, value] of Object.entries({ flushIntervalMs, requestTimeoutMs })) {
            if (!Number.isSafeInteger(value) || value < 1 || value > 2147483647) throw new RangeError(`${name} must be a positive timer duration in milliseconds`);
        }
        if (onError !== undefined && typeof onError !== 'function') throw new TypeError('onError must be a function');
        this.#baseUrl = `${base.href.replace(/\/+$/, '')}/service/integration/`;
        this.#token = token;
        this.#timeout = requestTimeoutMs;
        this.#onError = onError;
        this.flushIntervalMs = flushIntervalMs;
    }

    registerJob(key, manifest) {
        return this._operation(async () => {
            nonempty(key, 'key');
            object(manifest, 'manifest');
            if (manifest.key !== undefined && manifest.key !== key) throw new TypeError('manifest.key must match the job key');
            if (Object.hasOwn(manifest, 'steps')) throw new TypeError('Pass step definitions to job.startExecution(), not the manifest');
            const record = await this._request('POST', 'job/', { manifest: snapshot({ ...manifest, key }) });
            const job = new Job(this, record);
            this.#jobs.set(key, job);
            return job;
        });
    }

    getJob(key) {
        return this._operation(() => this.#findJob(nonempty(key, 'key')));
    }

    async #findJob(key) {
        const records = await this._request('GET', 'job/', undefined, { key, limit: 1 }, true);
        const record = records.find(record => record.key === key);
        if (!record) throw new IntegrationApiError(`Integration job not found: ${key}`, 404);
        const job = new Job(this, record);
        this.#jobs.set(key, job);
        return job;
    }

    listJobs(filter = {}) {
        return this._operation(async () => {
            object(filter, 'filter');
            for (const key of Object.keys(filter)) {
                if (!['key', 'afterKey', 'limit'].includes(key)) throw new TypeError(`Unknown job filter: ${key}`);
            }
            if (filter.limit !== undefined && (!Number.isInteger(filter.limit) || filter.limit < 1 || filter.limit > 1000)) throw new RangeError('limit must be between 1 and 1000');
            for (const key of ['key', 'afterKey']) if (filter[key] !== undefined) nonempty(filter[key], key);
            const records = await this._request('GET', 'job/', undefined, filter, true);
            return records.map(record => {
                const job = new Job(this, record);
                this.#jobs.set(job.key, job);
                return job;
            });
        });
    }

    deregisterJob(key) {
        return this._operation(async () => {
            nonempty(key, 'key');
            const job = this.#jobs.get(key) || await this.#findJob(key);
            await this._request('DELETE', `job/${encodeURIComponent(job.id)}`);
            this.#jobs.delete(key);
        });
    }

    async flush() {
        const results = await Promise.allSettled([...this.#executions].map(execution => execution.flush()));
        const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
        if (errors.length === 1) throw errors[0];
        if (errors.length) throw new AggregateError(errors, 'Failed to flush integration reports');
    }

    close() {
        if (this.#closed) return Promise.resolve();
        if (this.#closing) return this.#closing;
        this.#closing = (async () => {
            // Include executions whose start requests were already in flight.
            await Promise.allSettled([...this.#operations]);
            for (const execution of this.#executions) execution._stopTimers();
            await this.flush();
            this.#closed = true;
            this.#executions.clear();
        })().finally(() => { this.#closing = undefined; });
        return this.#closing;
    }

    _assertOpen() {
        if (this.#closed || this.#closing) throw new Error('Integration client is closing or closed');
    }

    _operation(fn) {
        let promise;
        try {
            this._assertOpen();
            promise = Promise.resolve(fn());
        } catch (error) {
            return Promise.reject(error);
        }
        this.#operations.add(promise);
        promise.then(() => this.#operations.delete(promise), () => this.#operations.delete(promise));
        return promise;
    }

    _attach(execution) { this.#executions.add(execution); }
    _detach(execution) { this.#executions.delete(execution); }

    _reportError(error) {
        if (this.#onError) {
            try {
                Promise.resolve(this.#onError(error)).catch(handlerError => {
                    console.error('[Hivekit SDK] onError handler failed:', handlerError);
                });
                return;
            } catch (handlerError) {
                console.error('[Hivekit SDK] onError handler failed:', handlerError);
            }
        }
        console.error('[Hivekit SDK] Background reporting failed:', error);
    }

    async _request(method, path, body, query, retry = false) {
        const url = new URL(path, this.#baseUrl);
        for (const [key, value] of Object.entries(query || {})) {
            if (value !== undefined) url.searchParams.set(key, String(value));
        }
        const serialized = body === undefined ? undefined : JSON.stringify(body);
        for (let attempt = 0; ; attempt++) {
            const token = tokenValue(typeof this.#token === 'function' ? await this.#token() : this.#token);
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), this.#timeout);
            timeout.unref();
            let error;
            try {
                const response = await fetch(url, {
                    method, body: serialized, signal: controller.signal, redirect: 'error',
                    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', Accept: 'application/json' }
                });
                const raw = await response.text();
                let payload;
                try { payload = JSON.parse(raw); } catch {
                    throw new IntegrationApiError('Integration server returned an invalid JSON response', response.status);
                }
                if (!payload || typeof payload !== 'object') throw new IntegrationApiError('Integration server returned an unexpected response', response.status);
                if (!response.ok || payload.result === 'error') {
                    throw new IntegrationApiError(typeof payload.message === 'string' ? payload.message : 'Integration request rejected', response.status);
                }
                if (payload.result === 'success') return payload.data;
                if (payload.res === 'suc') return;
                throw new IntegrationApiError('Integration server returned an unexpected response', response.status);
            } catch (cause) {
                error = cause instanceof IntegrationApiError ? cause : new IntegrationTransportError(
                    controller.signal.aborted ? 'Integration request timed out' : 'Integration request failed', cause);
            } finally {
                clearTimeout(timeout);
            }
            if (!retry || attempt >= 2 || (error instanceof IntegrationApiError && ![408, 429, 502, 503, 504].includes(error.status))) throw error;
            await new Promise(resolve => setTimeout(resolve, 100 * (2 ** attempt)));
        }
    }
}

class Job {
    #client;

    constructor(client, record) {
        this.#client = client;
        this.id = record.id;
        this.key = record.key;
        this.manifest = Object.freeze(snapshot(record.manifest));
        this.manifestRevision = record.manifestRevision;
        this.configRevision = record.configRevision;
        this.monitoringEnabled = record.monitoringEnabled;
    }

    startExecution(steps = [], options = {}) {
        return this.#client._operation(async () => {
            const definitions = stepDefinitions(steps);
            object(options, 'execution options');
            for (const key of Object.keys(options)) if (key !== 'metadata') throw new TypeError(`Unknown execution option: ${key}`);
            // Generated once per invocation and reused only for transport retries.
            const body = { idempotencyKey: randomUUID(), steps: definitions };
            if (options.metadata !== undefined) body.metadata = snapshot(options.metadata);
            const result = await this.#client._request('POST', `job/${encodeURIComponent(this.id)}/execution/`, body, undefined, true);
            const execution = new Execution(this.#client, this, result, definitions);
            this.#client._attach(execution);
            return execution;
        });
    }
}

module.exports = { IntegrationClient };
