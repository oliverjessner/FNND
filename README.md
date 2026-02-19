# NO BULLSHIT RSS

I vibe coded some electron slop.
check out the [Landing page](https://oliverjessner.at/no-bullshit-rss/#promise).

![overview of the app](/public/images/cards.webp)

No-Bullshit RSS is a minimal, open-source RSS reader that focuses on reading—not dashboards, upsells, or noise. It’s free, has no payments, and stores your feeds in a self-hosted database so you stay in control of your data.

highlights

- Self-hosted DB: your articles are stored in your own database
- Open source and no payment / no subscription / no ads
- Daily Digest: a clustered view that groups related articles for faster scanning
- Improved clustering: fuzzier matching with stronger logic and guardrails
- Instant search: highlight a word, right-click, and search it immediately
- Dark mode
- Storage visibility: settings now show how many articles are in your database

## run as electron

```bash
npm run electron
```

## build for electron

```bash
npm run dist:all:workaround
```
