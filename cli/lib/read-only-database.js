import sqlite3 from '@vscode/sqlite3';

export async function openReadOnlyDatabase(databasePath) {
    const connection = await new Promise((resolve, reject) => {
        const database = new sqlite3.Database(databasePath, sqlite3.OPEN_READONLY, error => {
            if (error) {
                reject(error);
                return;
            }
            resolve(database);
        });
    });

    return {
        all(sql, params = []) {
            return new Promise((resolve, reject) => {
                connection.all(sql, params, (error, rows) => {
                    if (error) {
                        reject(error);
                        return;
                    }
                    resolve(rows);
                });
            });
        },
        close() {
            return new Promise((resolve, reject) => {
                connection.close(error => {
                    if (error) {
                        reject(error);
                        return;
                    }
                    resolve();
                });
            });
        },
    };
}
