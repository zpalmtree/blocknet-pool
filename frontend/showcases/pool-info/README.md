# Pool info showcase

The real App on `/info` with synthetic API responses. Nothing reaches a real pool.

    npm --prefix frontend run showcase:info

Open `http://localhost:5203/showcases/pool-info/` and pick a state and theme from the bar,
or use the query directly:

- `state=ready` (default), `memo-off` (exchange memo payouts off), `loading`, `error`
- `theme=dark`, `lang=de` (any pool language), `chrome=off` hides the bar for captures
