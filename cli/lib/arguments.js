const HELP_TEXT = `NO BULLSHIT RSS CLI

Usage:
  no-bullshit-rss articles last <count> [--choose] [--url] [--title|--titles]
  no-bullshit-rss articles digest <count> [--daily|--weekly|--monthly]

Commands:
  articles last <count>      Show the newest stored articles
  articles digest <count>    Show the newest digest clusters as JSON

Options:
  --url                      Print one URL per line
  --title, --titles          Print one title per line
  --choose                   Interactively choose one of the newest articles
  --daily                    Use today's digest (default)
  --weekly                   Use this week's digest
  --monthly                  Use this month's digest
  -h, --help                 Show this help

When --url and --title are combined, each line is URL<TAB>TITLE.
Without a projection flag, "articles last" prints JSON.

Examples:
  no-bullshit-rss articles last 10 --url
  no-bullshit-rss articles last 10 --title
  no-bullshit-rss articles last 10 --url --title
  no-bullshit-rss articles last 10 --choose --url
  no-bullshit-rss articles digest 10 --daily
  no-bullshit-rss articles digest 10 --weekly
  no-bullshit-rss articles digest 10 --monthly`;

export function getHelpText() {
    return HELP_TEXT;
}

function parseCount(value) {
    if (!/^[1-9]\d*$/u.test(String(value || ''))) {
        throw new Error('<count> must be an integer greater than 0.');
    }

    const count = Number(value);
    if (!Number.isSafeInteger(count)) {
        throw new Error('<count> is too large.');
    }
    return count;
}

function rejectUnknownFlags(flags, allowedFlags) {
    const unknown = flags.find(flag => !allowedFlags.has(flag));
    if (unknown) {
        throw new Error(`Unknown option: ${unknown}`);
    }
}

export function parseCliArgs(argv) {
    const args = Array.isArray(argv) ? argv.map(value => String(value)) : [];
    if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
        return { command: 'help' };
    }

    const [resource, action, countValue, ...flags] = args;
    if (resource !== 'articles') {
        throw new Error(`Unknown resource: ${resource || '(missing)'}`);
    }
    if (!['last', 'digest'].includes(action)) {
        throw new Error(`Unknown articles command: ${action || '(missing)'}`);
    }
    if (countValue === undefined || countValue.startsWith('--')) {
        throw new Error('Missing required <count>.');
    }

    const count = parseCount(countValue);
    if (action === 'last') {
        rejectUnknownFlags(flags, new Set(['--url', '--title', '--titles', '--choose']));
        return {
            command: 'articles-last',
            count,
            url: flags.includes('--url'),
            title: flags.includes('--title') || flags.includes('--titles'),
            choose: flags.includes('--choose'),
        };
    }

    rejectUnknownFlags(flags, new Set(['--daily', '--weekly', '--monthly']));
    const selectedRanges = flags.filter(flag => ['--daily', '--weekly', '--monthly'].includes(flag));
    if (selectedRanges.length > 1) {
        throw new Error('Only one of --daily, --weekly or --monthly may be used.');
    }

    const rangeByFlag = {
        '--daily': 'day',
        '--weekly': 'week',
        '--monthly': 'month',
    };
    return {
        command: 'articles-digest',
        count,
        variant: rangeByFlag[selectedRanges[0]] || 'day',
    };
}
