# Get Started showcase

The real App on `/start` with synthetic API responses. Nothing reaches a real pool.

    npm --prefix frontend run showcase:start

Open `http://localhost:5202/showcases/get-started/` and pick a state, system and theme from the bar,
or use the query directly:

- `state=ready` (default), `solo-off` (latest Seine predates `solo:`), `loading`, `error`
- `os=windows|mac|linux` sets the visitor's system; without it the browser's own is used
- `theme=dark`, `lang=de` (any pool language), `chrome=off` hides the bar for captures
- `#solo-mining` opens the command row

The example address in the Seine screens has an invalid checksum, so Seine rejects it if copied.
