# gametracker

Romanian game price tracker CLI. On-demand lookups across 7 Romanian retailers for PS5 games.

## Install

```bash
pipx install git+https://github.com/<user>/gametracker.git
```

Nothing else needed — no browser binaries, no extra setup.

## Usage

```bash
gametracker check "Resident Evil Requiem"         # one-shot lookup
gametracker add "Resident Evil Requiem"           # save to favorites
gametracker favorites                              # check every favorite
gametracker list                                   # show saved favorites
gametracker remove "Resident Evil Requiem"        # drop from favorites
gametracker history "Resident Evil Requiem"       # price timeline per site
```

### Flags

```
--force                   bypass the 30-minute recheck cache
--sites altex,emag        limit to specific sites (comma-separated)
--verbose                 print per-site debug output to stderr
--<site>-cooldown N       override a site's min-gap-between-requests (seconds)
```

## Sites covered

altex, buy2play, emag, flanco, jocurinoi, ozone, trendyol

## How it works

- Parallel lookup across sites, respectful per-domain pacing, persisted cookie jar per site.
- Matches by fuzzy title, filters to PS5, picks the cheapest edition (or the edition you specified in the query).
- Stores observations in `~/.gametracker/db.sqlite`; historic low is per-site per-query.
- Status icons: `✓` found, `⚠` fallback strategy used, `✗` blocked/error, `—` no match.
