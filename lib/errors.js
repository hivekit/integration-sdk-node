'use strict';

class IntegrationApiError extends Error {
    constructor(message, status) {
        super(message);
        this.name = 'IntegrationApiError';
        this.status = status;
    }
}

class IntegrationTransportError extends Error {
    constructor(message, cause) {
        super(message, { cause });
        this.name = 'IntegrationTransportError';
    }
}

module.exports = { IntegrationApiError, IntegrationTransportError };
