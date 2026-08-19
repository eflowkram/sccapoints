# CLAUDE.md

Guidance for working in this repo.

## What this is

A single-purpose CLI that scrapes Axware autocross results into SQLite and prints
class and PAX standings for SoCal SCCA regions. Small, personal, no framework.

## Commands

```sh
pip install -r requirements-dev.txt   # includes runtime deps
pytest                                # full suite, ~2s
./clubpoints.py -u <url-or-file>      # scrape one results page
./clubpoints.py -c                    # class standings
./clubpoints.py --driver              # PAX standings
./clubpoints.py --debug -u ...        # verbose parsing
```

Progress and warnings go to **stderr**; standings go to stdout. `-c > out.txt`
captures just the data.

## Architecture

`clubpoints.py` is the whole program. Regional differences live in per-region
modules loaded by name from `config.ini`:

```python
ns = importlib.import_module(club)   # sdr.py or calclub.py
```

A region module must provide `points_card(number)`, `calc_points(fastest, driver)`
and `calc_drops(events)`; `load_region()` validates this at startup. Adding a
region means adding `<name>.py`, nothing else. **Never** put region-specific
branching in `clubpoints.py` — that is what the plugin split exists to avoid.

Region constants (`MIN_POINTS`, `DNF_POINTS`, `DROP_THRESHOLDS`,
`MIN_EVENT_FRACTION`, `NON_POINTS_CAR_RANGE`, `EVENTS_PER_DROP`) are module-level
and named. Prefer a named constant or a lookup table over an `if/elif` ladder;
Cal Club's drop schedule is `DROP_THRESHOLDS` for exactly this reason.

Authoritative sources for the rules:

- **Cal Club** — [Supplementary Regulations][ccregs]. Section 7 covers points and
  awards; Appendix D is the drop schedule. `calclub.py` cites section numbers
  inline; keep doing that when changing it. Export as text with
  `curl "https://docs.google.com/document/d/<id>/export?format=txt"`.
- **SDR** — <https://sdrscca.com/solorules/>, which returned HTTP 410 (the whole
  domain does, including its root, so this is likely a bot block rather than a
  dead page). `sdr.py` is therefore unverified against a published source and is
  annotated as such. Verify it if you can reach the page.

[ccregs]: https://docs.google.com/document/d/1FnBz9Qk0yMbst3tdN9hcd_3b-BEDclXNJUjPXK14XlE/edit

The two regions genuinely disagree — Cal Club scores a DNF as 0 and needs half
the season; SDR scores it 70 and needs two thirds. Never collapse a rule into a
shared constant because the regions happen to agree today.

## Domain rules

These come from the maintainer and are not derivable from the code. Do not
"simplify" them away.

- **Points are annual.** January starts at zero. Each season gets its own
  database, `<club>_<year>_points.db`. Mixing 2022 and 2026 results is refused,
  not merged.
- **Drops come from events *held*, not events attended.** Run 3 of 12 and you
  keep all 3 — the season's drops are absorbed by the events you missed. Run all
  12 and you lose your lowest 4 (SDR). This is why `generate_points()` writes
  zero-point `missed` placeholder rows.
- **A share of the events** must be run to score and be eligible for end-of-year
  trophies (`MIN_EVENT_FRACTION`): one half at Cal Club (Supp. Regs 7.6), two
  thirds at SDR. Ineligible drivers still appear in standings with `-` instead of
  a place. An event awarded an average counts as attended.
- **Non-points cars still set the benchmark.** Cal Club awards nothing to
  600-series numbers, but their times curve everyone else. `points_card()` is
  therefore checked only when a result is about to be recorded — never before the
  benchmark is taken. Do not move that check earlier.
- **NOV (novice) is a real scoring class** at Cal Club, indexed on PAX/RTP.
  Novices run 600-series numbers, so they start scoring once they take a
  permanent number. Never add `NOV` to `non_points`.
- **X = eXtra runs, TO = Time Only.** Neither is competing, neither scores.
  X entries also carry an X-suffixed car number (`681X`).
- **DNFs differ by region.** SDR: starting one run is worth at least 70 however
  it goes, so `MIN_POINTS` and `DNF_POINTS` are the same number. Cal Club: Supp.
  Regs 7.3.1 makes a DNF zero. A DNS is a third case in both — not a start,
  records nothing. Read the award via `dnf_points()`; never hardcode it back into
  `clubpoints.py`.
- **Axware owns the arithmetic.** It adds two seconds per cone and applies the
  PAX/RTP index, then writes the answer to `Total`. Read that column; never
  recompute a run time here. (Cal Club tried a "first three runs only" rule for
  the PAX class; the exports do not apply it, and neither should this code.)
- **National events** get a placeholder row holding the driver's average across
  every event they ran that year, recalculated whenever points are generated.

## Parsing Axware

**Axware's column layout is not stable.** Across captured results, the class
table's `Total` column lands at index 10, 11 or 12 depending on the export, and
the PAX sheet has three shapes — two of them the same width with different
meanings. Some events omit the date from the page header entirely.

So: **resolve columns by name from each table's header row**, never by counting
in from either end. `class_columns()` and `pax_columns()` do this. A fixed
"second to last column" rule reads the final run instead of the best time, which
is wrong and often fatal — `65.833+1` is not a float.

Score on the `Total` column. Axware has already applied cone penalties and, for
indexed classes (PAX, PAXL, ST, SP, XS, S1, HST1, NOV), the index factor. No
per-class arithmetic belongs in this codebase.

The Axware HTML comment (`Generated by: AXWare TS ... -2024-05-01`) is the
**software build date**, not the event date. Do not use it.

## Tests

`tests/fixtures/real/` holds unmodified Axware exports covering every known
layout; `SOURCE.md` there explains what each one demonstrates. Those are the
tests that matter — prefer adding a real capture over hand-writing HTML.

The two pages directly under `tests/fixtures/` are hand-written in the real
layout and exist only for cases the captures lack (DNS, blank time, double-digit
cones, all-runs-DNF, over-cutoff car number).

Scoring functions in `sdr.py`/`calclub.py` are pure — test them directly.

## Conventions

- SQL is parameterized. Never interpolate into a query string.
- Use `log.info/warning/debug`, not `print()`. `print()` is reserved for
  standings output.
- `execute_query` tolerates `IntegrityError` so re-scraping a URL is idempotent;
  every other database error is fatal.
- Prefer real captured data over invented data when verifying a change.
