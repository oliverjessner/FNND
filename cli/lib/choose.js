import readline from 'node:readline';

function truncate(value, maxLength) {
    const text = String(value || '').replace(/\s+/gu, ' ').trim();
    if (text.length <= maxLength) {
        return text;
    }
    return `${text.slice(0, Math.max(maxLength - 1, 1))}…`;
}

function buildArticleLabel(article, width) {
    const source = String(article?.sourceName || '').trim();
    const title = String(article?.title || 'Untitled').trim() || 'Untitled';
    return truncate(source ? `[${source}] ${title}` : title, Math.max(width - 4, 20));
}

function renderSelector(output, articles, selectedIndex, previousLineCount) {
    const terminalRows = Number(output.rows) || 24;
    const terminalColumns = Number(output.columns) || 100;
    const visibleCount = Math.min(articles.length, Math.max(terminalRows - 4, 3));
    const maxStart = Math.max(articles.length - visibleCount, 0);
    const startIndex = Math.min(Math.max(selectedIndex - Math.floor(visibleCount / 2), 0), maxStart);
    const visibleArticles = articles.slice(startIndex, startIndex + visibleCount);
    const lines = [
        'Choose an article (↑/↓ or j/k, Enter to select, Esc/q to cancel)',
        ...visibleArticles.map((article, offset) => {
            const articleIndex = startIndex + offset;
            const marker = articleIndex === selectedIndex ? '>' : ' ';
            return `${marker} ${buildArticleLabel(article, terminalColumns)}`;
        }),
        `${selectedIndex + 1}/${articles.length}`,
    ];

    if (previousLineCount > 0) {
        readline.moveCursor(output, 0, -(previousLineCount - 1));
        readline.cursorTo(output, 0);
    }

    lines.forEach((line, index) => {
        readline.clearLine(output, 0);
        output.write(line);
        if (index < lines.length - 1) {
            output.write('\n');
        }
    });

    return lines.length;
}

export async function chooseArticle(articles, { input = process.stdin, output = process.stderr } = {}) {
    if (!Array.isArray(articles) || articles.length === 0) {
        throw new Error('No articles available to choose from.');
    }
    if (!input?.isTTY || !output?.isTTY || typeof input.setRawMode !== 'function') {
        throw new Error('--choose requires an interactive terminal.');
    }

    readline.emitKeypressEvents(input);
    const wasRaw = Boolean(input.isRaw);
    let selectedIndex = 0;
    let renderedLineCount = 0;

    input.setRawMode(true);
    input.resume();
    output.write('\x1B[?25l');
    renderedLineCount = renderSelector(output, articles, selectedIndex, renderedLineCount);

    return new Promise((resolve, reject) => {
        const finish = (error, article) => {
            input.removeListener('keypress', onKeypress);
            input.setRawMode(wasRaw);
            if (!wasRaw) {
                input.pause();
            }
            output.write('\x1B[?25h\n');

            if (error) {
                reject(error);
                return;
            }
            resolve(article);
        };

        const onKeypress = (_character, key = {}) => {
            if (key.ctrl && key.name === 'c') {
                finish(new Error('Selection cancelled.'));
                return;
            }
            if (key.name === 'escape' || key.name === 'q') {
                finish(new Error('Selection cancelled.'));
                return;
            }
            if (key.name === 'return' || key.name === 'enter') {
                finish(null, articles[selectedIndex]);
                return;
            }
            if (key.name === 'up' || key.name === 'k') {
                selectedIndex = (selectedIndex - 1 + articles.length) % articles.length;
                renderedLineCount = renderSelector(output, articles, selectedIndex, renderedLineCount);
                return;
            }
            if (key.name === 'down' || key.name === 'j') {
                selectedIndex = (selectedIndex + 1) % articles.length;
                renderedLineCount = renderSelector(output, articles, selectedIndex, renderedLineCount);
                return;
            }
            if (key.name === 'home') {
                selectedIndex = 0;
                renderedLineCount = renderSelector(output, articles, selectedIndex, renderedLineCount);
                return;
            }
            if (key.name === 'end') {
                selectedIndex = articles.length - 1;
                renderedLineCount = renderSelector(output, articles, selectedIndex, renderedLineCount);
            }
        };

        input.on('keypress', onKeypress);
    });
}
