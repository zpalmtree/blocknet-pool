# BNTpool social preview showcase

Source: zero-fee metadata change based on `0126c3b`. The gallery reads the real production PNG assets and shared metadata directly. These static cards have no loading, empty, error, or disabled variants. Desktop (1280px) and mobile (390px) captures are included.

Start from the repository root:

```sh
python3 -m http.server 8878 --bind 0.0.0.0
```

Stable URL: http://localhost:8878/frontend/showcases/social-preview/

The homepage and Get Started descriptions advertise the 0% pool fee across search, Open Graph, Twitter and all 20 supported languages. The fee wording is scoped to the pool.

The homepage card was edited with the built-in imagegen tool, then resized to 1200×630. Prompt: replace only “Low fees · fast PPLNS payouts” with “0% pool fee · fast PPLNS payouts”; preserve the original layout, branding, colors and other text.

Validation: production frontend build and localization checks passed. 28 of 29 frontend tests passed; the existing rewards calculator test expects Seine v0.2.39 while the existing release data is v0.2.42. Neither file changed here.
