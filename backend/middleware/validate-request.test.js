import assert from 'node:assert/strict';
import test from 'node:test';
import { positiveIdParamsSchema, validateRequest } from './validate-request.js';

function invokeValidation(schemas, request) {
    let responseStatus = null;
    let responseBody = null;
    let calledNext = false;
    const response = {
        status(status) {
            responseStatus = status;
            return this;
        },
        json(body) {
            responseBody = body;
            return this;
        },
    };

    validateRequest(schemas)(request, response, () => {
        calledNext = true;
    });

    return { calledNext, responseStatus, responseBody };
}

test('accepts requests that match every configured schema', () => {
    const result = invokeValidation({ params: positiveIdParamsSchema }, { params: { id: '42' } });
    assert.equal(result.calledNext, true);
    assert.equal(result.responseStatus, null);
});

test('rejects invalid request input without echoing its value', () => {
    const result = invokeValidation({ params: positiveIdParamsSchema }, { params: { id: 'not-an-id' } });
    assert.equal(result.calledNext, false);
    assert.equal(result.responseStatus, 400);
    assert.equal(result.responseBody.error, 'Invalid request params');
    assert.equal(JSON.stringify(result.responseBody).includes('not-an-id'), false);
});
