function isLocalAddress(value) {
    const address = String(value || '').toLowerCase();
    return (
        address === 'localhost' ||
        address === '::1' ||
        address === '::ffff:127.0.0.1' ||
        address.startsWith('127.')
    );
}

export function requireLocalApiClient(req, res, next) {
    const remoteAddress = req.socket?.remoteAddress || req.ip;
    if (!isLocalAddress(remoteAddress)) {
        return res.status(403).json({ error: 'Local API access only' });
    }

    req.auth = {
        userId: 'local-user',
        ownerId: 'local-owner',
        scope: 'local-api',
    };

    return next();
}

export const auth = requireLocalApiClient;
