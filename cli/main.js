import { queryArticles } from '../backend/services/article-queries.js';
import { readStoredDigestPayload } from '../backend/services/digest-reader.js';
import { parseCliArgs, getHelpText } from './lib/arguments.js';
import { chooseArticle as chooseArticleInteractively } from './lib/choose.js';
import { discoverDatabasePath } from './lib/database-path.js';
import { readStoredFeeds } from './lib/feeds.js';
import { readStoredListArticles, readStoredLists, readStoredTopics } from './lib/library.js';
import { formatChosenArticle, formatDigest, formatFeeds, formatLastArticles } from './lib/output.js';
import { openReadOnlyDatabase } from './lib/read-only-database.js';

function writeLine(stream, value) {
    stream.write(`${value}\n`);
}

function writeResult(stream, value) {
    if (!value) {
        return;
    }
    writeLine(stream, value);
}

export async function runCli(
    argv,
    {
        stdout = process.stdout,
        stderr = process.stderr,
        stdin = process.stdin,
        cwd = process.cwd(),
        env = process.env,
        platform = process.platform,
        homeDir,
        discoverDatabase = discoverDatabasePath,
        openDatabase = openReadOnlyDatabase,
        chooseArticle = chooseArticleInteractively,
    } = {},
) {
    let database;
    let exitCode = 0;
    try {
        const command = parseCliArgs(argv);
        if (command.command === 'help') {
            writeLine(stdout, getHelpText());
            return exitCode;
        }

        const databasePath = await discoverDatabase({ cwd, env, platform, homeDir });
        database = await openDatabase(databasePath);
        const schema = await database.get("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'digest_periods'");
        if (!schema) throw new Error('Database schema v2 is required; use data-v2.db or restore the legacy archive separately.');
        const feedColumns = await database.all('PRAGMA table_info(feeds)');
        database.feedNamesAvailable = feedColumns.some(column => column.name === 'name');

        if (command.command === 'rss') {
            const feeds = await readStoredFeeds(database);
            writeResult(stdout, formatFeeds(feeds, command));
        } else if (command.command === 'topics') {
            writeResult(stdout, JSON.stringify(await readStoredTopics(database), null, 2));
        } else if (command.command === 'lists' && command.listName) {
            writeResult(stdout, formatLastArticles(await readStoredListArticles(database, command.listName)));
        } else if (command.command === 'lists') {
            writeResult(stdout, JSON.stringify(await readStoredLists(database), null, 2));
        } else if (command.command === 'articles-last') {
            const articles = await queryArticles(
                {
                    limit: command.count,
                    offset: 0,
                    activeOnly: false,
                    includeTopics: false,
                    maxLimit: null,
                },
                database,
            );
            if (command.choose) {
                const selectedArticle = await chooseArticle(articles, { input: stdin, output: stderr });
                writeResult(stdout, formatChosenArticle(selectedArticle, command));
            } else {
                writeResult(stdout, formatLastArticles(articles, command));
            }
        } else if (command.command === 'articles-digest') {
            const payload = await readStoredDigestPayload(command.variant, database);
            writeResult(stdout, formatDigest(payload, command.count));
        }
    } catch (error) {
        writeLine(stderr, `Error: ${error?.message || String(error)}`);
        exitCode = 1;
    } finally {
        if (database) {
            try {
                await database.close();
            } catch (error) {
                writeLine(stderr, `Error closing database: ${error?.message || String(error)}`);
                exitCode = 1;
            }
        }
    }
    return exitCode;
}
