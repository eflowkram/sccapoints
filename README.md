# SCCA club points

A little Python program to calculate class points for our local regions here in
SoCal. I wanted to figure out where I stood in points without having to manually
calculate everything, plus I wanted to try some web scraping.

It scrapes Axware results pages into a SQLite database and prints class and PAX
(driver) standings.

## Requirements

Python 3.9+. Developed and used on macOS and Linux.

## Installation

```sh
pip install -r requirements.txt
```

For the test suite, install the dev requirements instead:

```sh
pip install -r requirements-dev.txt
```

## Configuration

Edit `config.ini` and pick a region:

```ini
[region]
club = sdr         # or calclub
non_points = X,TO  # classes that are run but do not score
```

`non_points` is matched exactly against the class name in the results, so it has
to use the names that region's exports actually print — `X,TO` for Cal Club,
`X,TO,N` for SDR.

- **`X`** is eXtra runs: someone taking extra laps rather than competing. These
  entries also carry an X-suffixed car number such as `681X`, which the parser
  skips on its own regardless of this setting.
- **`TO`** is Time Only: someone out enjoying their car at an autocross without
  competing against anyone.

Note that `NOV` is **not** in that list: novice is a real scoring class at Cal
Club. Novices are given 600-series car numbers, which `points_card()` already
excludes, so they start scoring once they take a permanent number. Listing `NOV`
as a non-points class would drop the novice standings entirely.

`club` selects the scoring rules module (`sdr.py`, `calclub.py`) and names the
database file. SDR and Cal Club differ in how points are scored *and* in how many
events a driver may drop, which is why each region gets its own module.

Add a region by dropping a `<region>.py` next to `clubpoints.py` that provides
`points_card(number)`, `calc_points(fastest, driver)` and `calc_drops(events)`.

Point the script at a different config with `--config`.

## Seasons

Points are annual — January starts at zero and a new season begins — so each
season gets its own database, named `<club>_<year>_points.db`. Nothing needs to
be reset between years and nothing can leak across them.

You rarely need to name the season. Scraping infers it from the event date in
the results, and `-a` infers it from `-d`. Only reading back an older season
needs `--season`:

```sh
./clubpoints.py -u 01-25-2026-Class.htm   # writes calclub_2026_points.db
./clubpoints.py -c                        # newest season on disk
./clubpoints.py --season 2022 -c          # last year's standings
```

A command whose inputs span two seasons is refused rather than silently mixed,
and adding an event to a database that already holds a different year is an
error.

> Upgrading from an older checkout: databases used to be `<club>_points.db` with
> no year. Rename yours to `<club>_<year>_points.db` and it will be picked up;
> the script warns if it finds an unrenamed one.

## Usage

### 1. Scrape results

The database is created on first run. Feed it one results URL at a time:

```sh
./clubpoints.py -u https://sdrscca.com/solo2/results/2022/event-02%202022-02-27-Final_Web.htm
```

A local `.html` file works too. Enter events in chronological order. It is
easiest to save the URLs to a file, one per line, and loop:

```sh
while read -r url; do ./clubpoints.py -u "$url"; done < sdr_urls
```

Keep two lists: one of driver (PAX) points URLs and one of class URLs.
Re-scraping a URL you already have is a no-op, so a failed loop is safe to rerun.

### 2. Print standings

```sh
./clubpoints.py -c              # class standings
./clubpoints.py -c -n CS        # just one class
./clubpoints.py --driver        # driver (PAX) standings
./clubpoints.py --driver -o csv -f pax.csv
```

Points are regenerated from the raw results every time standings are printed, so
`-g` is only needed if you want to rebuild the tables without printing anything.

Progress and warnings go to stderr, so `./clubpoints.py -c > standings.txt`
captures just the standings.

### 3. National events

Both regions award participants their local average when they attend a national
event that conflicts with a local one. Give the car number, class and event date:

```sh
./clubpoints.py -a 97 -n CS -d 04-03-2022
```

That creates an entry in both the driver (PAX) and class results and fills in the
driver's average based on the local events they did attend in that class. The
driver must already exist in the database, so scrape at least one event they ran
first. See `national_avg.sh` for a batch example.

## Switches

| Switch | Description |
| --- | --- |
| `-u`, `--url` | URL to scrape, or a path to a local HTML file |
| `-a`, `--average` | Car number of a driver who ran a national event. Requires `-n` and `-d` |
| `-n`, `--name` | Class name, e.g. `CS`, `PAX`, `M1`. Also narrows `-c` to one class |
| `-d`, `--event_date` | Event date as `MM-DD-YYYY` |
| `-g`, `--generate` | Rebuild the class and driver points tables |
| `-c`, `--class_points` | Print class standings |
| `--driver` | Print driver (PAX) standings |
| `-o`, `--output` | Output format, `text` (default) or `csv` |
| `-f`, `--file` | Write output to this file instead of stdout |
| `--season` | Season (year) to work on. Inferred from the results or `-d` when scraping; needed only to read back an older season |
| `--config` | Path to the config file (default `config.ini`) |
| `--debug` | Verbose logging |

## Development

```sh
pytest
```

The scoring rules in `sdr.py` and `calclub.py` are pure functions and are
covered directly.

`tests/fixtures/real/` holds unmodified Axware exports covering both column
layouts seen in 2026 — those are the regression tests that matter. The two pages
directly under `tests/fixtures/` are hand-written in the same shape, and exist
only to cover cases the real files happen not to contain (a DNS, a blank time, a
double-digit cone count, an all-runs-DNF total, a car number over the points
cutoff).

## Scoring rules

**Points** come from the region module. SDR scores a percentage of the class
winner's time with a floor of 70; Cal Club falls off from 100 at four times the
percentage a driver is off the winner, with a floor of 0.

**DNFs differ by region.** At SDR, starting a single run is worth at least 70
points however the run goes — the same 70 as the scoring floor, so a very slow run
and a DNF both land there. Cal Club is the opposite: Supp. Regs 7.3.1 makes a DNF
(and any negative result) zero for that event. Both are `DNF_POINTS` in the region
module. A DNS is a third case in both regions — they never started, so nothing is
recorded at all.

**Cone penalties and indexing are Axware's job.** The timing software adds two
seconds per cone and applies the PAX/RTP index, then writes the result to the
`Total` column. This code reads that column and never recomputes a time.

**Seasons** are annual and independent. Points reset in January; 2022 results
never contribute to 2026 standings.

**Drops** are derived from the number of events *held* that season, not the
number a driver ran. SDR allows one drop per three events, so a twelve-event season has four.

- Run all 12 and your four lowest scores are dropped.
- Run 3 of 12 and you keep all three: the four drops are absorbed by the nine
  events you missed.
- Run 10 of 12 and two drops are absorbed by absences, so you lose your two
  lowest real scores.

This is why `generate_points()` writes a zero-point `missed` placeholder row for
every event a driver did not attend.

**Eligibility.** A driver must complete a share of the events held to score and
to be eligible for end-of-year trophies (`MIN_EVENT_FRACTION` in the region
modules) — **one half** at Cal Club (Supp. Regs 7.6), two thirds at SDR. An event
a driver qualifies for an average at counts as attended. Drivers below that still appear in the standings with their points, so
you can see where you stand mid-season, but they are shown with `-` instead of a
finishing position and are skipped when numbering places. CSV output carries
`Events` and `Eligible` columns instead.

**Indexed classes.** PAX and PAXL — and combined classes such as ST, SP, XS, S1,
HST1 and NOV — are scored on the indexed time rather than raw elapsed time. Both
land in the results table's `Total` column, so the parser reads that column
either way.

**Non-points cars still set the benchmark.** Cal Club awards no points to
600-series numbers, but their times count when calculating everyone else's. If a
600-series car wins a class outright, the drivers behind it are curved against
that time and nobody scores 100. Same on the PAX sheet. `points_card()` is
therefore checked only when the result is about to be recorded, never before the
benchmark is taken.

**National events.** A driver who runs a national event instead of a local one
gets a placeholder row for the event they missed, holding the average of every
event they did run that year. The average is recalculated from scratch each time
points are generated, so it tracks the driver as the season fills in. Placeholder
rows for events a driver simply skipped are flagged `missed` and excluded from
that average.

## Parsing notes

Axware's column layout is not stable between events. Across the captured results
the class table's trailing columns appear as `Team, S/T, Factor, Run 1..4, Total`
(2022), `Car Color, Run 1..4, Total, Diff.` or `S/T, Factor, Run 1..4, Total`
(2026) — which puts `Total` at index 12, 10 or 11 respectively. The PAX sheet has
three shapes too: 11 columns with `#` at index 2, 11 columns with an extra `Pos.`
that moves `#` to index 3, and a 9-column form. Some events omit the date from
the page header entirely.

Reading the best time as "the second-to-last column" lands on the final run
instead, which is usually wrong and sometimes fatal — a run recorded as
`65.833+1` cannot be parsed as a float, which aborts the whole class file.

So the parsers resolve columns by name from each table's header row rather than
by position, and fall back to the filename for a missing date. If a future export
breaks, compare its header row against `tests/fixtures/real/SOURCE.md` first.

## Rules sources

Cal Club's scoring formula (7.3), zero-points rule (7.3.1), class average (7.4),
eligibility threshold (7.6) and drop schedule (Appendix D) are all implemented
directly from the [Cal Club Autocross Supplementary Regulations][ccregs] and
carry section references in `calclub.py`.

[ccregs]: https://docs.google.com/document/d/1FnBz9Qk0yMbst3tdN9hcd_3b-BEDclXNJUjPXK14XlE/edit

SDR publishes supplemental rules at <https://sdrscca.com/solorules/>, which was
returning HTTP 410 when this was written. Everything in `sdr.py` therefore comes
from the original implementation and the maintainer's knowledge, not from a
verified reading, and is marked as such.

## Known rough edges

- **The overall (PAX) average does not follow Supp. Regs 7.5.** That section
  gives `OA = T / (C - n)`, where `C` is the number of events *held* and `n` the
  number awarded an average — so events missed without an average count as zeros
  in the denominator. This code instead averages over events attended, which is
  the class-average formula from 7.4. The class side is right; the overall side
  is more generous than the rules.
- **DNR is treated as absence, not as a zero.** Supp. Regs 7.3.1 counts "did not
  run" as zero points for the event. A `DNS` in the results is currently skipped
  entirely, so it becomes a `missed` placeholder and is excluded from averages
  rather than counted as a scored zero.
- **DNW (did not work) is not modelled.** Supp. Regs disqualifies a driver who
  does not work their assignment, but that is not present in the results HTML.
- In mobile-format exports a class is only detected on a `1T` row, so a class
  whose winner has no trophy marker may be missed. There is no mobile-format
  export among the captured fixtures to test this against.
- SDR's own results are not represented in the fixtures — the 2022 archive on
  sdrscca.com now returns 410, so only Cal Club exports could be captured.
