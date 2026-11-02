'use strict';

function nonempty(value, name) {
    if (typeof value !== 'string' || !value.trim()) {
        throw new TypeError(`${name} must be a non-empty string`);
    }
    return value;
}

function object(value, name) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new TypeError(`${name} must be an object`);
    }
}

// Snapshot queued data now: later caller mutations must not change reports or
// the body of a retried execution start. Also avoid silently rounding integers.
function snapshot(value) {
    return JSON.parse(JSON.stringify(value, (key, entry) => {
        if (typeof entry === 'number' && (!Number.isFinite(entry) || (Number.isInteger(entry) && !Number.isSafeInteger(entry)))) {
            throw new RangeError('JSON numbers must be finite and integers must be within the JavaScript safe range');
        }
        if (entry instanceof Error) {
            return { name: entry.name, message: entry.message, stack: entry.stack };
        }
        return entry;
    }));
}

function reasonText(reason) {
    return nonempty(reason instanceof Error ? reason.message : reason, 'reason');
}

function stepDefinitions(steps) {
    if (!Array.isArray(steps) || steps.length > 1000) {
        throw new TypeError('steps must be an array of at most 1000 step definitions');
    }
    const keys = new Set();
    return steps.map(step => {
        object(step, 'step');
        const key = nonempty(step.key, 'step.key');
        if (keys.has(key)) throw new TypeError(`Duplicate step key: ${key}`);
        keys.add(key);
        const name = step.name === undefined ? key : nonempty(step.name, 'step.name');
        const weight = step.weight === undefined ? 1 : step.weight;
        if (!Number.isFinite(weight) || weight <= 0 || weight > 1 || Number(weight.toFixed(6)) === 0) {
            throw new RangeError('Step weights must be positive fractions up to 1, representable with six decimal places');
        }
        return { key, name, weight: Number(weight.toFixed(6)) };
    });
}

module.exports = { nonempty, object, snapshot, reasonText, stepDefinitions };
