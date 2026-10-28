# Dashboard SEO and aggregate intent measurement

The dashboard keeps its existing header, controls, cards, charts and tables. A
native collapsed guide follows recent payouts. `PoolGuide.ts` renders the same
localized content into initial HTML and React. Payout thresholds remain linked
to the live Pool Information section, avoiding another hardcoded configuration.

The collector accepts fixed `home` pages for both sites and `pool-start` for the
pool, plus existing article slugs. Actions add `setup_guide` and `worker_stats`.
The optional `source` is one of search, direct, internal or referral, classified
locally from the document referrer. It sends no URL, query, address or identity.
Search includes recognized search referrals, not verified organic attribution;
direct includes missing referrers. There is no cross-page or cross-site matching.
SPA page mounts count as views; revisits/reloads can count again. The report
prints both legacy five-part keys and new six-part keys (source appended).
Existing rate, body-size, retention and aggregate-cardinality limits remain.

The new module entry is `site-events.js`; keep the legacy `article-metrics.js`
so cached pre-deploy HTML continues to execute its original classic script.
Both sites carry identical `site-metrics.mjs` modules and contract tests.
No live events were submitted during preview verification.

Deployment order when authorized: pool API/UI first (`--api-only`), then the
Seine site. The API accepts old clients throughout. No Stratum or daemon change
is required. Seine's website normally deploys automatically on master pushes;
the implementation commit uses [skip ci] to honor the separate deploy permission.
Publish the updated website privacy disclosure with this change.

Validation and visual captures: frontend/showcases/seo-usability/README.md.
