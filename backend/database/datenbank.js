import sqlite3 from '@vscode/sqlite3';
import { readFile } from 'node:fs/promises';
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

export async function initSchema() {
    const schemaPath = path.join(__dirname, 'schema.sql');
    const schemaSql = await readFile(schemaPath, 'utf-8');

    return new Promise((resolve, reject) => {
        db.exec(schemaSql, err => {
            if (err) return reject(err);
            resolve();
        });
    });
}

export default db;
