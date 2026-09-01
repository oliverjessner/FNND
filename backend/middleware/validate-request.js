import Ajv from 'ajv';

const ajv = new Ajv({ allErrors: true, strict: true });

function formatErrors(errors = []) {
    return errors.map(error => ({
        path: error.instancePath || '/',
        message: error.message || 'is invalid',
    }));
}

export function validateRequest(schemas) {
    const validators = Object.entries(schemas).map(([requestPart, schema]) => [requestPart, ajv.compile(schema)]);

    return function validateRequestMiddleware(req, res, next) {
        for (const [requestPart, validate] of validators) {
            if (!validate(req[requestPart])) {
                return res.status(400).json({
                    error: `Invalid request ${requestPart}`,
                    details: formatErrors(validate.errors),
                });
            }
        }

        return next();
    };
}

export const requestSchema = Object.freeze({ validate: validateRequest });

export const positiveIdParamsSchema = {
    type: 'object',
    required: ['id'],
    properties: {
        id: { type: 'string', pattern: '^[1-9]\\d*$' },
    },
    additionalProperties: true,
};

export const articleIdsSchema = {
    type: 'array',
    minItems: 1,
    maxItems: 10_000,
    items: {
        anyOf: [
            { type: 'integer', minimum: 1 },
            { type: 'string', pattern: '^[1-9]\\d*$' },
        ],
    },
};
