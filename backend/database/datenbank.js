import sqlite3 from '@vscode/sqlite3';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', '..', 'data.db');
const db = new sqlite3.Database(DB_PATH);

db.serialize(() => db.run('PRAGMA foreign_keys = ON'));

export function run(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.run(sql, params, function (err) {
            if (err) return reject(err);
            resolve({ lastID: this.lastID, changes: this.changes });
        });
    });
}

export function get(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.get(sql, params, (err, row) => {
            if (err) return reject(err);
            resolve(row);
        });
    });
}

export function all(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.all(sql, params, (err, rows) => {
            if (err) return reject(err);
            resolve(rows);
        });
    });
}

function execSql(sql) {
    return new Promise((resolve, reject) => {
        db.exec(sql, err => {
            if (err) return reject(err);
            resolve();
        });
    });
}

export async function initSchema() {
    const migrationsDir = path.join(__dirname, '..', '..', 'migrations');
    const migrationFiles = (await readdir(migrationsDir))
        .filter(file => /^\d+_.+\.sql$/u.test(file))
        .sort((left, right) => left.localeCompare(right));

    await run(
        `CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            appliedAt TEXT NOT NULL DEFAULT (datetime('now'))
        )`,
    );

    for (const file of migrationFiles) {
        const applied = await get('SELECT version FROM schema_migrations WHERE version = ?', [file]);
        if (applied) {
            continue;
        }

        const migrationSql = await readFile(path.join(migrationsDir, file), 'utf-8');
        await execSql(migrationSql);
        await run('INSERT INTO schema_migrations (version, appliedAt) VALUES (?, datetime(\'now\'))', [file]);
    }
}

export default db;
