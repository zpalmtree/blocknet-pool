# Miner troubleshooting and article measurement

2026-10-25. Expanded the existing `track-your-miner-on-bntpool` article in all
20 languages. It owns “seine mining but pool hashrate zero”; do not create a
competing Seine post. The existing public-dashboard cover remains appropriate.
Translations remain machine-assisted drafts; a separate reviewer is optional.

Evidence: Seine `src/pool.rs` contains `failed to connect to pool at` and
`submit interrupted during reconnect`; `src/miner/mining_pool.rs` handles
`login rejected:` and `stale job`. `docs/guides/configuration.md` documents
shortname resolution at startup. Existing tracking-guide research documents
hourly accepted-share estimates, variable arrival times and My Stats refreshes.
Zero estimated rewards is distinct from zero hashrate: link to the PPLNS owner.

Contextual links now appear in pool setup, My Stats, Seine downloads and both
GitHub READMEs. All public UI link labels follow the selected language.

## Reporting

The two article templates load the same small script. It counts one view and at
most one of each click action per document load. Only article-contained links to
Seine downloads/releases and pool setup qualify; navigation and unrelated links
do not. Local previews, Do Not Track and Global Privacy Control disable collection.

`POST /api/site-events` accepts only two exact production Origins, known articles,
20 locales and three action names. Its body is limited to 1 KiB. The independent
100 requests/second budget and 65,000 aggregate-key cap bound resource use. It
never calls the database from a request handler or stores individual events.
Origins and counts are untrusted and spoofable; these metrics are editorial
signals, not billing, unique visitors or proof of completed downloads/mining.

One background writer saves the aggregate map in PostgreSQL meta every 30 seconds
when dirty. It loads successfully before accepting events, retries load/write
failures, and prunes to 30 daily UTC buckets. A restart can lose the unflushed
batch. Nightly DB backups can retain snapshots for another seven days. Website
privacy details are published at https://seineminer.com/privacy/#website-analytics
and linked from both blogs. The miner's hardware telemetry is separate.

Read `GET /api/admin/site-metrics` with the existing operator API key. It is behind
the same authentication middleware as the other admin endpoints. No public report
or new dashboard is exposed. Use `scripts/site_metrics_report.py --input report.json`
or set `BNTPOOL_API_KEY` and run the script; it prints last persistence in Eastern
time and totals by site/article/language. It never prints the key. `counts` keys
are `UTC_epoch_day|site|article|locale|action`; the raw JSON supports day-level review.

When adding an article, update `apps/blocknet-pool-api/src/site_articles.json`.
Deploy the API collector before enabling a newly registered Seine article client.
This is an API-only change: deploy with `--api-only` and verify Stratum is unchanged.
