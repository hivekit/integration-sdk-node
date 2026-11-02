'use strict';

const { nonempty, reasonText, snapshot } = require('./values');

const MAX_QUEUE_ENTRIES = 10000;
const MAX_QUEUE_BYTES = 8 * 1024 * 1024;
const MAX_BATCH_BYTES = 1024 * 1024;
const LOG_BATCH_SIZE = 100;

class Execution {
    #client;
    #job;
    #status;
    #steps = new Map();
    #queue = [];
    #queueBytes = 0;
    #sendingCount = 0;
    #flushing;
    #finishing;
    #finishStatus;
    #flushTimer;
    #heartbeatTimer;
    #heartbeat;
    #lastError;

    constructor(client, job, result, definitions) {
        this.#client = client;
        this.#job = job;
        this.id = result.execution.id;
        this.#status = result.execution.status;
        this.config = Object.freeze(snapshot(result.configuration));
        this.manifestRevision = result.execution.manifestRevision;
        this.configRevision = result.execution.configRevision;
        for (const definition of definitions) this.#steps.set(definition.key, new Step(this, definition));
        this.#startFlushTimer();
        this.#startHeartbeats();
    }

    get status() { return this.#status; }

    startStep(key) {
        this._assertRunning();
        const step = this.#steps.get(nonempty(key, 'step key'));
        if (!step) throw new Error(`Step was not declared in startExecution(): ${key}`);
        return step._start();
    }

    debug(message, details) { this._log('debug', message, details); }
    info(message, details) { this._log('info', message, details); }
    warn(message, details) { this._log('warn', message, details); }
    error(message, details) { this._log('error', message, details); }

    _assertRunning() {
        this.#client._assertOpen();
        if (this.#status !== 'running' || this.#finishing) throw new Error('Execution is completing or already complete');
    }

    _log(level, message, details, stepKey) {
        nonempty(message, 'log message');
        if (Buffer.byteLength(message) > 16384) throw new RangeError('Log messages must be at most 16384 UTF-8 bytes');
        const occurredAt = new Date().toISOString();
        const prefix = `[${this.#job.key}]${stepKey === undefined ? '' : `[${stepKey}]`} ${message}`;
        // Mirror synchronously at the call site, before any batching or network
        // work. The host's stdout/stderr capture remains available after a crash.
        if (details === undefined) console[level](prefix);
        else console[level](prefix, details);

        this.#client._assertOpen();
        if (this.#finishing) throw new Error('Execution is completing');
        const entry = { level, message, occurredAt };
        if (stepKey !== undefined) entry.stepKey = stepKey;
        if (details !== undefined) entry.details = snapshot(details);
        const bytes = Buffer.byteLength(JSON.stringify(entry));
        if (bytes > MAX_BATCH_BYTES - 32) throw new RangeError('A log entry must fit within a 1 MiB request');
        this.#append({ type: 'log', body: entry, bytes });
        if (this.#queue.length >= LOG_BATCH_SIZE) this.#flushInBackground();
    }

    _updateStep(key, body) {
        this._assertRunning();
        const bytes = Buffer.byteLength(JSON.stringify(body));
        const last = this.#queue.at(-1);
        // Coalesce only unsent running updates. Mutating an in-flight entry
        // would lose progress when that earlier request is acknowledged.
        if (last && this.#queue.length > this.#sendingCount && last.type === 'step' && last.key === key && last.body.status === 'running' && body.status === 'running') {
            if (this.#queueBytes + bytes - last.bytes > MAX_QUEUE_BYTES) throw new RangeError('Integration reporting queue is full; await execution.flush() before adding more reports');
            this.#queueBytes += bytes - last.bytes;
            last.body = body;
            last.bytes = bytes;
            return;
        }
        this.#append({ type: 'step', key, body, bytes });
    }

    #append(entry) {
        if (this.#queue.length >= MAX_QUEUE_ENTRIES || this.#queueBytes + entry.bytes > MAX_QUEUE_BYTES) {
            throw new RangeError('Integration reporting queue is full; await execution.flush() before adding more reports');
        }
        this.#queue.push(entry);
        this.#queueBytes += entry.bytes;
        this.#client._attach(this);
        this.#startFlushTimer();
    }

    async flush() {
        // Every caller observes the same ordered drain. Failed reports remain
        // queued, so a later explicit flush/close can recover a temporary outage.
        while (this.#queue.length || this.#flushing) {
            if (!this.#flushing) {
                this.#flushing = this.#drain().finally(() => { this.#flushing = undefined; });
            }
            await this.#flushing;
        }
        if (this.#heartbeat) await this.#heartbeat;
        if (this.#status !== 'running') {
            this._stopTimers();
            this.#client._detach(this);
        }
    }

    async #drain() {
        while (this.#queue.length) {
            const first = this.#queue[0];
            let count = 1;
            let method = 'PUT';
            let path = `execution/${encodeURIComponent(this.id)}/step/${encodeURIComponent(first.key)}`;
            let body = first.body;
            if (first.type === 'log') {
                let bytes = first.bytes + 32;
                while (count < this.#queue.length && count < LOG_BATCH_SIZE && this.#queue[count].type === 'log' && bytes + this.#queue[count].bytes + 1 <= MAX_BATCH_BYTES) {
                    bytes += this.#queue[count].bytes + 1;
                    count++;
                }
                method = 'POST';
                path = `execution/${encodeURIComponent(this.id)}/log/`;
                body = { logs: this.#queue.slice(0, count).map(entry => entry.body) };
            }
            this.#sendingCount = count;
            try {
                await this.#client._request(method, path, body, undefined, first.type === 'step');
                for (const entry of this.#queue.splice(0, count)) this.#queueBytes -= entry.bytes;
                this.#lastError = undefined;
            } finally {
                this.#sendingCount = 0;
            }
        }
    }

    #backgroundError(error) {
        if (this.#lastError !== error.message) this.#client._reportError(error);
        this.#lastError = error.message;
    }

    #flushInBackground() {
        this.flush().catch(error => this.#backgroundError(error));
    }

    #startFlushTimer() {
        if (this.#flushTimer) return;
        this.#flushTimer = setInterval(() => this.#flushInBackground(), this.#client.flushIntervalMs);
        this.#flushTimer.unref();
    }

    #startHeartbeats() {
        if (this.#heartbeatTimer || this.#status !== 'running') return;
        const seconds = this.#job.manifest.heartbeatTimeoutSeconds;
        const interval = Number.isFinite(seconds) && seconds > 0 ? Math.max(250, Math.min(10000, Math.floor(seconds * 1000 / 3))) : 10000;
        this.#heartbeatTimer = setInterval(() => {
            if (this.#heartbeat) return;
            this.#heartbeat = this.#client._request('POST', `execution/${encodeURIComponent(this.id)}/heartbeat`, {}, undefined, true)
                .catch(error => this.#backgroundError(error))
                .finally(() => { this.#heartbeat = undefined; });
        }, interval);
        this.#heartbeatTimer.unref();
    }

    _stopTimers() {
        clearInterval(this.#flushTimer);
        clearInterval(this.#heartbeatTimer);
        this.#flushTimer = this.#heartbeatTimer = undefined;
    }

    complete() {
        return this.#client._operation(() => this.#finish('succeeded'));
    }

    fail(reason, details) {
        return this.#client._operation(() => this.#finish('failed', reasonText(reason), details === undefined && reason instanceof Error ? reason : details));
    }

    #finish(status, reason, details) {
        if (this.#status !== 'running') {
            if (this.#status !== status) return Promise.reject(new Error('Execution already completed with a different status'));
            return this.flush();
        }
        if (this.#finishing) {
            if (this.#finishStatus !== status) return Promise.reject(new Error('Execution is completing with a different status'));
            return this.#finishing;
        }
        if (status === 'succeeded' && [...this.#steps.values()].some(step => !['succeeded', 'skipped'].includes(step.status))) {
            return Promise.reject(new Error('Every step must be complete or skipped before completing the execution'));
        }
        if (status === 'failed') this.error(reason, details);
        this.#finishStatus = status;
        clearInterval(this.#heartbeatTimer);
        this.#heartbeatTimer = undefined;
        this.#finishing = (async () => {
            await this.flush();
            const body = { status };
            if (reason !== undefined) body.errorSummary = reason;
            await this.#client._request('POST', `execution/${encodeURIComponent(this.id)}/complete`, body, undefined, true);
            this.#status = status;
            this._stopTimers();
            this.#client._detach(this);
        })().finally(() => {
            this.#finishing = undefined;
            if (this.#status === 'running') this.#startHeartbeats();
        });
        return this.#finishing;
    }
}

class Step {
    #execution;
    #status = 'pending';
    #progress = 0;

    constructor(execution, { key, name, weight }) {
        this.#execution = execution;
        this.key = key;
        this.name = name;
        this.weight = weight;
    }

    get status() { return this.#status; }
    get progressValue() { return this.#progress; }

    _start() {
        if (this.#status === 'running') return this;
        if (this.#status !== 'pending') throw new Error(`Step already completed: ${this.key}`);
        this.#execution._updateStep(this.key, { status: 'running', progress: 0 });
        this.#status = 'running';
        return this;
    }

    progress(value) {
        this.#execution._assertRunning();
        if (this.#status !== 'running') throw new Error('Step is not running');
        if (!Number.isFinite(value) || value < 0 || value > 1) throw new RangeError('Progress must be a fraction between 0 and 1');
        const progress = Math.round(value * 100 * 1e6) / 1e6;
        if (progress <= this.#progress * 100) return;
        this.#execution._updateStep(this.key, { status: 'running', progress });
        this.#progress = progress / 100;
    }

    debug(message, details) { this.#execution._log('debug', message, details, this.key); }
    info(message, details) { this.#execution._log('info', message, details, this.key); }
    warn(message, details) { this.#execution._log('warn', message, details, this.key); }
    error(message, details) { this.#execution._log('error', message, details, this.key); }

    async complete() {
        this.#finish('succeeded');
        await this.#execution.flush();
    }

    async fail(reason, details) {
        this.#execution._assertRunning();
        const message = reasonText(reason);
        if (this.#status !== 'failed') {
            this.#finish('failed');
            this.error(message, details === undefined && reason instanceof Error ? reason : details);
        }
        await this.#execution.flush();
    }

    async skip(reason) {
        this.#finish('skipped');
        if (reason !== undefined) this.info(nonempty(reason, 'reason'));
        await this.#execution.flush();
    }

    #finish(status) {
        this.#execution._assertRunning();
        if (this.#status === status) return;
        if (this.#status !== 'running') throw new Error('Step already completed with a different status');
        const progress = status === 'failed' ? this.#progress : 1;
        this.#execution._updateStep(this.key, { status, progress: progress * 100 });
        this.#progress = progress;
        this.#status = status;
    }
}

module.exports = { Execution };
