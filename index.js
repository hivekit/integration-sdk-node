'use strict';

const { IntegrationClient } = require('./lib/client');
const { IntegrationApiError, IntegrationTransportError } = require('./lib/errors');

module.exports = { IntegrationClient, IntegrationApiError, IntegrationTransportError };
