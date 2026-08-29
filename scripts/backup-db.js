import os from 'node:os';
import path from 'node:path';
import { backupSqliteDatabase } from './lib/sqlite-backup.js';

function argumentValue(name) {
    const index = process.argv.indexOf(name);
    return index >= 0 ? process.argv[index + 1] : null;
}

const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
const sourcePath = path.resolve(argumentValue('--source') || process.env.DB_PATH || path.join(os.homedir(), 'Library', 'Application Support', 'NO-BULLSHIT-RSS', 'data-v2.db'));
const destinationPath = path.resolve(argumentValue('--destination') || path.join(path.dirname(sourcePath), 'backups', `data-${timestamp}.db`));

backupSqliteDatabase(sourcePath, destinationPath)
    .then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(error => {
        console.error(`Database backup failed: ${error.message}`);
        process.exitCode = 1;
    });
