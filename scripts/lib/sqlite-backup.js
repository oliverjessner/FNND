import sqlite3 from '@vscode/sqlite3';
import { mkdir, stat } from 'node:fs/promises';
import path from 'node:path';

function openDatabase(filename, mode) {
    return new Promise((resolve, reject) => {
        const database = new sqlite3.Database(filename, mode, error => (error ? reject(error) : resolve(database)));
    });
}

function closeDatabase(database) {
    return new Promise((resolve, reject) => database.close(error => (error ? reject(error) : resolve())));
}

function get(database, sql) {
    return new Promise((resolve, reject) => database.get(sql, (error, row) => (error ? reject(error) : resolve(row))));
}

async function ensureSourceFile(sourcePath) {
    const info = await stat(sourcePath);
    if (!info.isFile()) throw new Error(`Not a database file: ${sourcePath}`);
}

export async function verifySqliteDatabase(databasePath) {
    const database = await openDatabase(databasePath, sqlite3.OPEN_READONLY);
    try {
        const integrity = await get(database, 'PRAGMA integrity_check');
        const foreignKeys = await get(database, 'PRAGMA foreign_key_check');
        if (String(integrity?.integrity_check || '').toLowerCase() !== 'ok') {
            throw new Error(`Integrity check failed for ${databasePath}: ${JSON.stringify(integrity)}`);
        }
        return { integrity: 'ok', foreignKeyViolation: foreignKeys || null };
    } finally {
        await closeDatabase(database);
    }
}

export async function backupSqliteDatabase(sourcePath, destinationPath) {
    await ensureSourceFile(sourcePath);
    await mkdir(path.dirname(destinationPath), { recursive: true });
    const database = await openDatabase(sourcePath, sqlite3.OPEN_READONLY);
    try {
        await new Promise((resolve, reject) => {
            let backup;
            backup = database.backup(destinationPath, error => {
                if (error) return reject(error);
                backup.step(-1, (stepError, done) => {
                    if (stepError) return reject(stepError);
                    if (!done) return reject(new Error('SQLite backup did not complete'));
                    backup.finish(finishError => (finishError ? reject(finishError) : resolve()));
                });
            });
        });
    } finally {
        await closeDatabase(database);
    }
    const verification = await verifySqliteDatabase(destinationPath);
    return { sourcePath, destinationPath, ...verification };
}
