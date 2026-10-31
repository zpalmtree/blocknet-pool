# Pool navigation showcase

Source base: `d1dd758`; App.tsx blob `a7c1b4800fd5fa920a11a0851e8e905b283fc350`.
Status is removed from the shared menu in every locale. Its direct route remains.
The menu does not have loading, error, empty or disabled states of its own.
Desktop and expanded mobile navigation were captured and reviewed through the
Windows Playwright MCP at 1366px and 390px; neither has horizontal overflow.

Gallery: http://localhost:5192/

```sh
python3 -m http.server 5192 --directory /home/zp/Code/blocknet/showcases/pool-navigation
```

Interactive fixture from the repository root:
`npm --prefix frontend run showcase:ui`, then open http://localhost:5186/.
