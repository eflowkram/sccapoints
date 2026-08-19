#!/usr/bin/env python3
"""SCCA club points calculator.

Scrapes Axware results pages into a SQLite database and prints class and PAX
(driver) standings. Regional scoring differences live in a per-region module
(``sdr.py``, ``calclub.py``) selected by ``club`` in config.ini.
"""

import argparse
import csv
import datetime
import glob
import importlib
import logging
import math
import os
import re
import sqlite3
import sys
from configparser import ConfigParser, NoOptionError, NoSectionError
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("clubpoints")

DEFAULT_CONFIG = "config.ini"
HTTP_TIMEOUT = 30

# Every region module must provide these.
REQUIRED_REGION_FUNCS = ("points_card", "calc_points", "calc_drops")

# Fallback DNF award for a region module that does not declare one.
DEFAULT_DNF_POINTS = 70

# Shown instead of a finishing position for drivers who have not run enough
# events to score.
INELIGIBLE_MARK = "-"

# Set by load_config(): the region rules module and the non-scoring classes.
REGION = None
NON_POINTS = ()

# Axware writes a run as "45.123+2" for cones and "45.123+DNF" for a DNF.
CONE_PATTERN = re.compile(r"\+(\d+)")
DNF_PATTERN = re.compile(r"\+DNF")
DATE_PATTERN = re.compile(r"\d{2}-\d{2}-\d{4}")
# Class tables head their run columns "Run 1", "Run 2.." and so on.
RUN_HEADER_PATTERN = re.compile(r"^Run\s*\d+", re.I)

# Combined-class prefixes, longest first so PAXL wins over PAX.
CLASS_PREFIXES = ("PAXL", "PAX", "HST1", "HST2", "SP", "XS", "P")

SCHEMA = (
    """
CREATE TABLE IF NOT EXISTS class_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date date,
    driver_id int(10),
    class varchar(10),
    place int(10),
    final_time decimal(10,3),
    points decimal(10,3),
    cones int(10),
    dnf int(10),
    national BOOLEAN DEFAULT 0 CHECK (national IN (0, 1)),
    missed BOOLEAN DEFAULT 0 CHECK (missed IN (0, 1)),
    unique (event_date,driver_id,class)
);
""",
    """
CREATE TABLE IF NOT EXISTS drivers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_name varchar(50),
    car_number int(10) unique
);
""",
    """
CREATE TABLE IF NOT EXISTS class_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id int(10),
    class varchar(10),
    points decimal(10,3),
    cones int(10),
    dnf int(10),
    unique (driver_id,class)
);
""",
    """
CREATE TABLE IF NOT EXISTS driver_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date date,
    driver_id int(10),
    class varchar(10),
    place int(10),
    final_time decimal(10,3),
    points decimal(10,3),
    national BOOLEAN DEFAULT 0 CHECK (national IN (0, 1)),
    missed BOOLEAN DEFAULT 0 CHECK (missed IN (0, 1)),
    unique (event_date,driver_id)
);
""",
    """
CREATE TABLE IF NOT EXISTS driver_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id int(10),
    points decimal(10,3),
    unique (driver_id)
);
""",
)


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


class _Formatter(logging.Formatter):
    """Plain text for progress, a level tag for anything that needs attention."""

    def format(self, record):
        if record.levelno == logging.INFO:
            return record.getMessage()
        return f"[{record.levelname}] {record.getMessage()}"


def setup_logging(debug=False):
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_Formatter())
    log.handlers[:] = [handler]
    log.setLevel(logging.DEBUG if debug else logging.INFO)


def load_region(name):
    """Import a region rules module and check it implements the interface."""
    global REGION
    try:
        module = importlib.import_module(name)
    except ImportError as exc:
        sys.exit(
            f"Unknown region '{name}': {exc}\n"
            f"Expected a module named {name}.py next to this script."
        )
    missing = [f for f in REQUIRED_REGION_FUNCS if not callable(getattr(module, f, None))]
    if missing:
        sys.exit(f"Region module '{name}' is missing: {', '.join(missing)}")
    REGION = module
    return module


def load_config(path=DEFAULT_CONFIG):
    """Read config.ini, import the region module, return the club name."""
    global NON_POINTS
    if not os.path.isfile(path):
        sys.exit(
            f"Config file '{path}' not found.\n"
            f"Create it with a [region] section and set 'club' to sdr or calclub."
        )
    config = ConfigParser()
    config.read(path)
    try:
        club = config.get("region", "club").strip()
    except (NoSectionError, NoOptionError) as exc:
        sys.exit(
            f"{path} is incomplete: {exc}\n"
            f"It needs a [region] section with 'club' set to sdr or calclub."
        )
    if not club:
        sys.exit(f"{path}: 'club' is empty. Set it to sdr or calclub.")
    NON_POINTS = tuple(
        c.strip().upper()
        for c in config.get("region", "non_points", fallback="").split(",")
        if c.strip()
    )
    load_region(club)
    return club


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------


def create_connection(path):
    try:
        return sqlite3.connect(path)
    except sqlite3.Error as exc:
        sys.exit(f"Could not open database '{path}': {exc}")


def execute_query(connection, sql, params=()):
    """Run a write query. Returns the new rowid, or None if it was a duplicate."""
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params)
        connection.commit()
        return cursor.lastrowid
    except sqlite3.IntegrityError as exc:
        # Re-scraping an event we already hold trips a UNIQUE constraint; that
        # makes re-running a URL idempotent rather than an error.
        log.debug("skipped duplicate row (%s): %r", exc, params)
        return None
    except sqlite3.Error as exc:
        sys.exit(f"Database error: {exc}\nQuery: {sql}\nParams: {params!r}")


def execute_read_query(connection, sql, params=()):
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params)
        return cursor.fetchall()
    except sqlite3.Error as exc:
        sys.exit(f"Database error: {exc}\nQuery: {sql}\nParams: {params!r}")


def season_year(event_date):
    """The season an event belongs to. Dates are MM-DD-YYYY."""
    return event_date[-4:]


def database_name(club, year):
    return f"{club}_{year}_points.db"


def known_seasons(club):
    """Seasons this club already has a database for, oldest first."""
    years = []
    for path in glob.glob(f"{club}_*_points.db"):
        year = path[len(club) + 1 : -len("_points.db")]
        if year.isdigit():
            years.append(year)
    return sorted(years)


def default_season(club):
    """Which season to work on when the command line does not imply one."""
    seasons = known_seasons(club)
    if seasons:
        return seasons[-1]
    return str(datetime.date.today().year)


def warn_about_legacy_database(club):
    """Point out a pre-season-split database rather than silently ignoring it."""
    legacy = f"{club}_points.db"
    if not os.path.isfile(legacy):
        return
    log.warning(
        "found %s from before points were split by season; it is not being "
        "read. Rename it to %s to keep using it.",
        legacy,
        database_name(club, "<year>"),
    )


def assert_same_season(connection, event_date):
    """Points are annual: refuse to mix seasons in one database."""
    year = season_year(event_date)
    existing = set()
    for table in ("class_results", "driver_results"):
        rows = execute_read_query(connection, f"SELECT DISTINCT event_date FROM {table}")
        existing.update(season_year(row[0]) for row in rows)
    other = sorted(existing - {year})
    if other:
        sys.exit(
            f"Refusing to add a {year} event to a database holding "
            f"{', '.join(other)}. Points are annual and January starts at zero, "
            f"so each season needs its own database."
        )


def db_init(database_name):
    connection = create_connection(database_name)
    for table in SCHEMA:
        execute_query(connection, table)
    return connection


def get_or_create_driver(connection, driver, car_number):
    """Return the drivers.id for a car number, inserting the driver if new."""
    execute_query(
        connection,
        "INSERT OR IGNORE INTO drivers VALUES (NULL,?,?)",
        (driver, car_number),
    )
    rows = execute_read_query(
        connection, "SELECT id FROM drivers WHERE car_number = ?", (car_number,)
    )
    return rows[0][0]


def event_dates(connection):
    """Every event in the season, oldest first.

    Dates are stored MM-DD-YYYY, so they have to be reordered to YYYYMMDD to
    sort. Without this the Event 1..N columns follow whatever order the results
    were scraped in.
    """
    rows = execute_read_query(
        connection,
        "SELECT DISTINCT event_date FROM class_results "
        "ORDER BY substr(event_date,7,4) || substr(event_date,1,2) "
        "|| substr(event_date,4,2)",
    )
    return [row[0] for row in rows]


# --------------------------------------------------------------------------
# Points calculation
# --------------------------------------------------------------------------


def _average_points(connection, table, where_sql, params):
    """Mean of the points column for matching rows, or None if there are none."""
    rows = execute_read_query(
        connection, f"SELECT points FROM {table} WHERE {where_sql}", params
    )
    if not rows:
        return None
    return round(sum(row[0] for row in rows) / len(rows), 3)


def update_average_points(connection, driver_id, car_class):
    """Award a driver their local average for an event missed for a national."""
    rows = execute_read_query(
        connection,
        "SELECT count(1) FROM class_results WHERE driver_id = ? AND national = 1",
        (driver_id,),
    )
    if rows[0][0] == 0:
        return

    avg = _average_points(
        connection,
        "class_results",
        "class = ? AND driver_id = ? AND national = 0 AND missed = 0",
        (car_class, driver_id),
    )
    if avg is None:
        log.warning(
            "driver_id %s has no scored %s results to average; leaving the "
            "national entry at 0 points",
            driver_id,
            car_class,
        )
    else:
        log.debug("driver_id: %s class: %s class average: %s", driver_id, car_class, avg)
        execute_query(
            connection,
            "UPDATE class_results SET points = ? "
            "WHERE class = ? AND driver_id = ? AND national = 1",
            (avg, car_class, driver_id),
        )

    driver_avg = _average_points(
        connection,
        "driver_results",
        "driver_id = ? AND national = 0 AND missed = 0",
        (driver_id,),
    )
    if driver_avg is None:
        log.warning(
            "driver_id %s has no scored PAX results to average; leaving the "
            "national entry at 0 points",
            driver_id,
        )
        return
    log.debug("driver_id: %s pax average: %s", driver_id, driver_avg)
    execute_query(
        connection,
        "UPDATE driver_results SET points = ? WHERE driver_id = ? AND national = 1",
        (driver_avg, driver_id),
    )


def events_attended(connection, driver_id, car_class=None):
    """How many events a driver actually ran, ignoring missed-event placeholders."""
    if car_class is None:
        sql = "SELECT count(1) FROM driver_results WHERE driver_id = ? AND missed = 0"
        params = (driver_id,)
    else:
        sql = (
            "SELECT count(1) FROM class_results "
            "WHERE driver_id = ? AND class = ? AND missed = 0"
        )
        params = (driver_id, car_class)
    return execute_read_query(connection, sql, params)[0][0]


def is_eligible(attended, held):
    """Whether a driver has run enough of the season to score.

    Regions require a driver to complete a share of the events run so far
    (two thirds in both SDR and Cal Club) before they place in the standings or
    qualify for end-of-year trophies.
    """
    fraction = getattr(REGION, "MIN_EVENT_FRACTION", 0)
    if not fraction:
        return True
    return attended >= math.ceil(fraction * held)


def dnf_points():
    """What this region awards a driver who started but set no clean time."""
    return float(getattr(REGION, "DNF_POINTS", DEFAULT_DNF_POINTS))


def _points_after_drops(points, drops):
    kept = sorted(points, reverse=True)[: max(0, len(points) - drops)]
    return round(sum(kept), 3)


def total_class_points(connection, driver_id, car_class):
    rows = execute_read_query(
        connection,
        "SELECT points FROM class_results WHERE driver_id = ? AND class = ?",
        (driver_id, car_class),
    )
    # Drops are derived from the number of events HELD, not the number the
    # driver ran, which is why the zero-point `missed` placeholders are counted
    # here. A driver who ran 3 of 12 keeps all 3: the season's 4 drops are
    # absorbed by the 9 events they missed. A driver who ran all 12 loses their
    # 4 lowest scores.
    drops = REGION.calc_drops(len(rows))
    total = _points_after_drops([row[0] for row in rows], drops)
    log.debug(
        "driver_id: %s class: %s events: %s drops: %s total: %s",
        driver_id,
        car_class,
        len(rows),
        drops,
        total,
    )
    return total


def total_driver_points(connection, driver_id):
    rows = execute_read_query(
        connection, "SELECT points FROM driver_results WHERE driver_id = ?", (driver_id,)
    )
    drops = REGION.calc_drops(len(rows))
    total = _points_after_drops([row[0] for row in rows], drops)
    log.debug(
        "driver_id: %s events: %s drops: %s pax total: %s",
        driver_id,
        len(rows),
        drops,
        total,
    )
    return total


# --------------------------------------------------------------------------
# HTML parsing
# --------------------------------------------------------------------------


def table_data(table_handle):
    return [
        [cell.text.strip() for cell in row.find_all(["th", "td"])]
        for row in table_handle.find_all("tr")
    ]


def get_file_format(table_handle):
    for line in table_data(table_handle):
        for item in line:
            if "Mobile" in item:
                log.debug("mobile format detected")
                return "mobile"
    return "standard"


def get_event_date(table_handle, source=None):
    """Find the event date, falling back to the filename when the page omits it.

    Most Axware headers carry the date in the event title, but some events are
    titled by name only (e.g. "#1 - JAnuary Championship"). Those files are still
    named MM-DD-YYYY-Class.htm, so the URL is a reliable second source.
    """
    event_date = None
    for line in table_data(table_handle):
        result = DATE_PATTERN.search(str(line))
        if result is not None:
            event_date = result.group()
    if event_date is None and source:
        result = DATE_PATTERN.search(os.path.basename(source))
        if result is not None:
            event_date = result.group()
            log.warning(
                "no date in the results header, using %s from the filename",
                event_date,
            )
    if event_date is None:
        sys.exit("Event Date not found!")
    return event_date


def class_columns(header, width):
    """Locate the scoring and run columns of a class table from its header row.

    Axware right-aligns data under the class header: the header's first cell
    spans the leading identity columns (place, S/T, number, driver, car), so a
    header cell at index i lines up with data index i + (width - len(header)).
    The trailing block varies by event -- some carry Factor and S/T, others Car
    Color and a Diff. column -- so the columns have to be resolved per table
    rather than assumed by position.
    """
    offset = width - len(header)
    total_col, run_cols = None, []
    for i, cell in enumerate(header):
        if cell.strip().lower().startswith("total"):
            total_col = i + offset
        elif RUN_HEADER_PATTERN.match(cell.strip()):
            run_cols.append(i + offset)
    if total_col is None or not 0 <= total_col < width:
        # Unrecognised layout: fall back to the second-to-last column, which is
        # where the total sits on most exports.
        log.warning("no Total column in class header %r, guessing", header)
        total_col = width - 2
        # None, not [] -- let get_cone_dnf fall back to its legacy full-row scan
        # rather than silently counting no cones at all.
        run_cols = None
    return total_col, run_cols


def pax_columns(header):
    """Locate the PAX table's columns by name.

    The PAX sheet gained a raw-position and Total column part way through the
    season, which shifts Class, # and Driver one to the right, so these cannot
    be read off fixed indexes.
    """
    wanted = {"class": "class", "#": "number", "driver": "driver", "pax time": "time"}
    columns = {}
    for i, cell in enumerate(header):
        key = wanted.get(cell.strip().lower())
        if key is not None:
            columns.setdefault(key, i)
    missing = {"class", "number", "driver", "time"} - set(columns)
    if missing:
        # Pre-2026 layout: Class/#/Driver lead the row and the PAX time sits
        # third from the right.
        log.warning("PAX header %r missing %s, using legacy columns", header, sorted(missing))
        columns = {"class": 1, "number": 2, "driver": 3, "time": len(header) - 3}
    return columns


def get_cone_dnf(table_row, run_cols=None):
    """Count cones and DNFs across a driver's runs.

    Without explicit run columns this scans everything but the trailing total
    columns, which is what the older exports required.
    """
    if run_cols is None:
        run_cols = range(max(0, len(table_row) - 2))
    cones, dnf = 0, 0
    for i in run_cols:
        if i >= len(table_row):
            continue
        cell = str(table_row[i])
        match = CONE_PATTERN.search(cell)
        if match is not None:
            cones += int(match.group(1))
        if DNF_PATTERN.search(cell) is not None:
            dnf += 1
    return cones, dnf


def get_car_class(cc):
    """Collapse a combined-class entry (e.g. "PAX3") to its base class."""
    return next((prefix for prefix in CLASS_PREFIXES if cc.startswith(prefix)), cc)


def _parse_place(position, context):
    try:
        return int(position)
    except ValueError:
        log.warning("%s: unparseable place %r, skipping row", context, position)
        return None


def class_point_parser(connection, soup, event_date, mobile_format="standard"):
    class_data = table_data(soup.find_all("table")[2])
    car_class = ""
    class_header = None
    winner_time = None
    log.debug("parsing class results (format: %s)", mobile_format)

    for item in class_data:
        if len(item) == 0:
            continue
        first_element = item[0]
        if len(first_element) == 0:
            continue

        if first_element[0].isalpha():
            if mobile_format == "standard":
                # This row names the class and heads its columns.
                car_class = first_element.split(" ")[0]
                class_header = item
                # Each class is scored against its own winner.
                winner_time = None
                if car_class.upper() not in NON_POINTS:
                    log.info("Class: %s", car_class)
            continue

        if item[0] == "1T" and mobile_format == "mobile":
            new_class = get_car_class(item[1])
            if new_class != car_class:
                winner_time = None
            car_class = new_class
            if car_class.upper() not in NON_POINTS:
                log.info("Class: %s", car_class)

        if car_class.upper() in NON_POINTS:
            continue

        if class_header is not None:
            total_col, run_cols = class_columns(class_header, len(item))
        else:
            total_col, run_cols = len(item) - 2, None
        log.debug("columns: %s total column: %s runs: %s", len(item), total_col, run_cols)

        number = item[2]
        if number.endswith("X"):
            # "681X" marks eXtra runs -- someone taking extra laps for fun
            # rather than competing. Those do not score.
            log.debug("event %s: skipping extra-run entry %r", event_date, number)
            continue
        try:
            car_number = int(number)
        except ValueError:
            log.warning(
                "event %s class %s: unparseable car number %r, skipping row",
                event_date,
                car_class,
                number,
            )
            continue
        context = f"event {event_date} class {car_class} car {car_number}"
        place = _parse_place(first_element.replace("T", ""), context)
        if place is None:
            continue

        result = item[total_col]
        if result in ("DNS", ""):
            # Axware marks a no-show "DNS" and leaves the indexed time blank.
            log.debug("%s: no time recorded (%r), skipping row", context, result)
            continue

        driver = item[3].replace("'", "").title()
        if result == "DNF":
            final_time = 0.000
            points = dnf_points()
        else:
            try:
                final_time = float(result)
            except ValueError:
                log.warning("%s: unparseable time %r, skipping row", context, result)
                continue
            if final_time == 0.0:
                # Every run DNF'd; Axware writes the total as 0.000.
                points = dnf_points()
            else:
                if place == 1:
                    winner_time = final_time
                if winner_time is None:
                    # The outright winner does not earn points (a guest or
                    # non-points car number), so the curve starts at the
                    # fastest entry that does. Results are in time order, so
                    # this row is that entry.
                    log.info(
                        "%s: class winner does not score, curving from this "
                        "run (%.3f)",
                        context,
                        final_time,
                    )
                    winner_time = final_time
                points = REGION.calc_points(winner_time, final_time)

        cones, dnf = get_cone_dnf(item, run_cols)
        if not REGION.points_card(car_number):
            continue

        log.info(
            "Event Date: %s Position: %s Class: %s Car No: %s Driver: %s "
            "Points: %s Cones: %s DNF: %s",
            event_date,
            place,
            car_class,
            car_number,
            driver,
            points,
            cones,
            dnf,
        )
        driver_id = get_or_create_driver(connection, driver, car_number)
        execute_query(
            connection,
            "INSERT INTO class_results VALUES (NULL,?,?,?,?,?,?,?,?,0,0)",
            (event_date, driver_id, car_class, place, final_time, points, cones, dnf),
        )


def driver_point_parser(connection, soup, event_date):
    index = pax_table_index(soup)
    if index is None:
        index = 1  # older exports without the "Pax Pos." header
    pax_data = table_data(soup.find_all("table")[index])
    winner_time = None
    columns = None
    log.debug("parsing pax results")

    for item in pax_data:
        if len(item) == 0:
            continue
        first_element = item[0]
        if len(first_element) == 0:
            continue
        if first_element[0].isalpha():
            # The first such row is the column header; keep it for the layout.
            if columns is None:
                columns = pax_columns(item)
                log.debug("pax columns: %s", columns)
            continue
        if columns is None:
            log.warning("pax table has no header row, using legacy columns")
            columns = {"class": 1, "number": 2, "driver": 3, "time": len(item) - 3}

        number = item[columns["number"]]
        if number.endswith("X"):
            # eXtra runs, not a competitive entry.
            log.debug("event %s: skipping extra-run entry %r", event_date, number)
            continue
        try:
            car_number = int(number)
        except ValueError:
            log.warning(
                "event %s: unparseable car number %r, skipping row", event_date, number
            )
            continue
        car_class = item[columns["class"]]
        context = f"event {event_date} pax car {car_number}"
        place = _parse_place(first_element.replace("T", ""), context)
        if place is None:
            continue

        result = item[columns["time"]]
        if result in ("DNS", ""):
            # Axware marks a no-show "DNS" and leaves the indexed time blank.
            log.debug("%s: no time recorded (%r), skipping row", context, result)
            continue

        driver = item[columns["driver"]].replace("'", "").title()
        if result == "DNF":
            final_time = 0.000
            points = dnf_points()
        else:
            try:
                final_time = float(result)
            except ValueError:
                log.warning("%s: unparseable time %r, skipping row", context, result)
                continue
            if final_time == 0.0:
                points = dnf_points()
            else:
                if place == 1:
                    winner_time = final_time
                if winner_time is None:
                    log.info(
                        "%s: pax winner does not score, curving from this "
                        "run (%.3f)",
                        context,
                        final_time,
                    )
                    winner_time = final_time
                points = REGION.calc_points(winner_time, final_time)

        # Checked only now, so a non-points entry still sets the benchmark
        # everyone else is curved against -- same as the class results.
        if not REGION.points_card(car_number):
            continue

        log.info(
            "Event Date: %s Pax Position: %s Car No: %s Driver: %s Points: %s",
            event_date,
            place,
            car_number,
            driver,
            points,
        )
        driver_id = get_or_create_driver(connection, driver, car_number)
        execute_query(
            connection,
            "INSERT INTO driver_results VALUES (NULL,?,?,?,?,?,?,0,0)",
            (event_date, driver_id, car_class, place, final_time, points),
        )


def pax_table_index(soup):
    """Index of the PAX results grid, or None if this is not a PAX sheet.

    A PAX sheet heads its grid "Pax Pos."; a class sheet's second table is the
    class index ("PAX | PAXL | SS | AS ..."). That header is what actually tells
    the two apart -- table counts collide, since a 2022 class sheet and a 2026
    PAX sheet both have three.
    """
    for index, table in enumerate(soup.find_all("table")):
        rows = table_data(table)
        if rows and rows[0] and rows[0][0].strip().lower().startswith("pax pos"):
            return index
    return None


def select_parser(soup):
    """Pick the parser matching this export's layout."""
    if pax_table_index(soup) is not None:
        return driver_point_parser

    # No PAX header: fall back to counting tables, which is how this was done
    # before and still covers exports that head their grid differently.
    table_count = len(soup.find_all("table"))
    parsers = {
        2: driver_point_parser,
        3: class_point_parser,
        4: class_point_parser,
    }
    parser = parsers.get(table_count)
    if parser is None:
        sys.exit(f"Invalid Input: unrecognized layout with {table_count} tables")
    # A 3-table export is a PAX sheet when its second table is a full results
    # grid rather than a short class index.
    if table_count == 3 and len(table_data(soup.find_all("table")[1])) >= 5:
        return driver_point_parser
    return parser


def load_soup(url):
    if url.startswith("http"):
        response = requests.get(url, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")
    with open(url) as handle:
        return BeautifulSoup(handle, "html.parser")


def result_links(soup, base_url):
    """Result pages linked from a season index, oldest first.

    Axware result files are named MM-DD-YYYY-Class.htm / -PAX.htm, so a page
    that links to dated .htm files is an index rather than a results page.
    """
    seen, found = set(), []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        name = os.path.basename(href.split("?")[0].rstrip("/"))
        if not name.lower().endswith((".htm", ".html")):
            continue
        match = DATE_PATTERN.search(name)
        if match is None:
            continue
        url = urljoin(base_url, href)
        if url in seen:
            continue
        seen.add(url)
        found.append((match.group(), name, url))

    def chronological(item):
        date, name, _ = item
        month, day, year = date.split("-")
        # Class results before the PAX sheet for the same event, just to keep
        # the order deterministic.
        return (year, month, day, 0 if "class" in name.lower() else 1)

    return [url for _, _, url in sorted(found, key=chronological)]


def read_results(url, soup=None):
    """Load a results page and work out which event it is."""
    if soup is None:
        soup = load_soup(url)
    event_date = get_event_date(soup.find_all("table")[0], source=url)
    return soup, event_date


def parse_results(connection, soup, event_date):
    header = soup.find_all("table")[0]
    mobile_format = get_file_format(header)
    log.debug(
        "tables: %s format: %s event date: %s",
        len(soup.find_all("table")),
        mobile_format,
        event_date,
    )
    assert_same_season(connection, event_date)
    parser = select_parser(soup)
    if parser is class_point_parser:
        parser(connection, soup, event_date, mobile_format)
    else:
        parser(connection, soup, event_date)


def scrape(connection, url):
    soup, event_date = read_results(url)
    parse_results(connection, soup, event_date)


# --------------------------------------------------------------------------
# Standings
# --------------------------------------------------------------------------


def missed_events(connection, driver_id, car_class):
    """Insert zero-point placeholder rows for events a driver did not attend.

    The `missed` flag distinguishes these from a genuine zero-point result, so
    averaging can exclude them.
    """
    for date in event_dates(connection):
        rows = execute_read_query(
            connection,
            "SELECT count(1) FROM class_results "
            "WHERE driver_id = ? AND class = ? AND event_date = ?",
            (driver_id, car_class, date),
        )
        if rows[0][0] == 0:
            log.debug(
                "no %s result for driver_id %s on %s, creating placeholder",
                car_class,
                driver_id,
                date,
            )
            execute_query(
                connection,
                "INSERT INTO class_results VALUES (NULL,?,?,?,0,0,0,0,0,0,1)",
                (date, driver_id, car_class),
            )

        rows = execute_read_query(
            connection,
            "SELECT count(1) FROM driver_results WHERE driver_id = ? AND event_date = ?",
            (driver_id, date),
        )
        if rows[0][0] == 0:
            log.debug(
                "no pax result for driver_id %s on %s, creating placeholder",
                driver_id,
                date,
            )
            execute_query(
                connection,
                "INSERT INTO driver_results VALUES (NULL,?,?,NULL,0,0,0,0,1)",
                (date, driver_id),
            )


def generate_points(connection):
    """Rebuild the class_points and driver_points tables from raw results."""
    execute_query(connection, "DELETE FROM class_points")
    execute_query(connection, "DELETE FROM driver_points")

    driver_ids = execute_read_query(connection, "SELECT id FROM drivers ORDER BY id ASC")
    for (driver_id,) in driver_ids:
        classes = execute_read_query(
            connection,
            "SELECT DISTINCT class FROM class_results WHERE driver_id = ?",
            (driver_id,),
        )
        for (car_class,) in classes:
            missed_events(connection, driver_id, car_class)
            update_average_points(connection, driver_id, car_class)
            total_points = total_class_points(connection, driver_id, car_class)
            result = execute_read_query(
                connection,
                "SELECT sum(cones), sum(dnf) FROM class_results "
                "WHERE driver_id = ? AND class = ? AND missed = 0",
                (driver_id, car_class),
            )
            cones, dnf = result[0]
            execute_query(
                connection,
                "INSERT INTO class_points VALUES (NULL,?,?,?,?,?)",
                (driver_id, car_class, total_points, cones, dnf),
            )
        execute_query(
            connection,
            "INSERT INTO driver_points VALUES (NULL,?,?)",
            (driver_id, total_driver_points(connection, driver_id)),
        )


def driver_event_points(connection, car_number):
    """A driver's PAX points for each event, in event order, 0 where absent."""
    event_p = []
    for date in event_dates(connection):
        rows = execute_read_query(
            connection,
            "SELECT points FROM driver_results "
            "JOIN drivers ON driver_results.driver_id = drivers.id "
            "WHERE car_number = ? AND event_date = ?",
            (car_number, date),
        )
        event_p.append("0" if not rows else str(rows[0][0]))
    return event_p


def class_standings(connection, driver_id, car_class):
    """Return a driver's class summary row and their per-event points."""
    event_points = []
    for date in event_dates(connection):
        rows = execute_read_query(
            connection,
            "SELECT points FROM class_results "
            "WHERE driver_id = ? AND class = ? AND event_date = ?",
            (driver_id, car_class, date),
        )
        event_points.append(rows[0][0] if len(rows) == 1 else 0)

    rows = execute_read_query(
        connection,
        "SELECT driver_name, car_number, points, cones, dnf FROM class_points "
        "JOIN drivers ON drivers.id = class_points.driver_id "
        "WHERE class = ? AND driver_id = ?",
        (car_class, driver_id),
    )
    if not rows:
        return None, event_points
    name, car_number, points, cones, dnf = rows[0]
    return [name, car_number, car_class, points, cones, dnf], event_points


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------


def class_header_text(event_c):
    e = "".join(f"{'Event '}{i : <4}" for i in range(1, event_c + 1))
    return (
        f"\n{'Place' : <10}{'Driver' : <25}{'Car' : <8}{'Class' : <9}"
        + e
        + f"{'Points' : <10}{'Cones' : <8}{'DNF' : <5}"
    )


def driver_header_text(event_c):
    e = "".join(f"Event {i : <5}" for i in range(1, event_c + 1))
    return f"\n{'Place' : <10}{'Driver' : <20}{'Car' : <8}" + e + f"{'Points' : <7}"


def class_header_csv(event_c):
    return (
        ["Place", "Driver", "Car", "Class"]
        + [f"Event {i}" for i in range(1, event_c + 1)]
        + ["Points", "Cones", "DNF", "Events", "Eligible"]
    )


def driver_header_csv(event_c):
    return (
        ["Place", "Driver", "Car"]
        + [f"Event {i}" for i in range(1, event_c + 1)]
        + ["Points", "Events", "Eligible"]
    )


def eligibility_note(event_c):
    fraction = getattr(REGION, "MIN_EVENT_FRACTION", 0)
    needed = math.ceil(fraction * event_c)
    return (
        f"\n{INELIGIBLE_MARK} = has not run the {needed} of {event_c} events "
        f"needed to score and be eligible for end-of-year trophies."
    )


def print_class_standings(connection, car_classes, output, handle):
    event_c = len(event_dates(connection))
    writer = csv.writer(handle) if output == "csv" else None
    any_ineligible = False
    for car_class in car_classes:
        rows = execute_read_query(
            connection,
            "SELECT driver_id FROM class_points "
            "JOIN drivers ON drivers.id = class_points.driver_id "
            "WHERE class = ? ORDER BY points DESC",
            (car_class,),
        )
        if writer is not None:
            writer.writerow(class_header_csv(event_c))
        else:
            print(class_header_text(event_c), file=handle)

        place = 0
        for (driver_id,) in rows:
            row, event_points = class_standings(connection, driver_id, car_class)
            if row is None:
                log.warning(
                    "no class_points row for driver_id %s in %s, skipping",
                    driver_id,
                    car_class,
                )
                continue
            attended = events_attended(connection, driver_id, car_class)
            eligible = is_eligible(attended, event_c)
            if eligible:
                place += 1
                shown = place
            else:
                any_ineligible = True
                shown = INELIGIBLE_MARK
            log.debug("row %s, event points %s, ran %s", row, event_points, attended)
            if writer is not None:
                writer.writerow(
                    [shown, row[0], row[1], row[2], *event_points,
                     row[3], row[4], row[5], attended, "yes" if eligible else "no"]
                )
            else:
                epoints = "".join(f"{p:<10}" for p in event_points)
                line1 = f"{shown : <10}{row[0] : <25}{row[1] : <8}{row[2] : <9}"
                line2 = f"{row[3] : <10}{row[4] : <8}{row[5] : <8}"
                print(line1, epoints, line2, sep="", file=handle)
    if any_ineligible and writer is None:
        print(eligibility_note(event_c), file=handle)


def print_driver_standings(connection, output, handle):
    event_c = len(event_dates(connection))
    rows = execute_read_query(
        connection,
        "SELECT driver_points.driver_id, driver_name, car_number, points "
        "FROM driver_points JOIN drivers ON driver_points.driver_id = drivers.id "
        "ORDER BY points DESC",
    )
    writer = csv.writer(handle) if output == "csv" else None
    if writer is not None:
        writer.writerow(driver_header_csv(event_c))
    else:
        print(driver_header_text(event_c), file=handle)

    place = 0
    any_ineligible = False
    for driver_id, name, car_number, points in rows:
        attended = events_attended(connection, driver_id)
        eligible = is_eligible(attended, event_c)
        if eligible:
            place += 1
            shown = place
        else:
            any_ineligible = True
            shown = INELIGIBLE_MARK
        event_points = driver_event_points(connection, car_number)
        if writer is not None:
            writer.writerow(
                [shown, name, car_number, *event_points, points, attended,
                 "yes" if eligible else "no"]
            )
        else:
            rendered = "".join(f"{p : <11}" for p in event_points)
            print(
                f"{shown : <10}{name : <20}{car_number : <8}{rendered}{points : <8}",
                sep="",
                file=handle,
            )
    if any_ineligible and writer is None:
        print(eligibility_note(event_c), file=handle)


# --------------------------------------------------------------------------
# National average entries
# --------------------------------------------------------------------------


def record_national_average(connection, car_number, car_class, event_date):
    """Flag (or create) a driver's entry for an event missed for a national."""
    rows = execute_read_query(
        connection,
        "SELECT class_results.id, drivers.id FROM class_results "
        "JOIN drivers ON drivers.id = driver_id "
        "WHERE event_date = ? AND class = ? AND car_number = ?",
        (event_date, car_class, car_number),
    )
    if not rows:
        driver_rows = execute_read_query(
            connection, "SELECT id FROM drivers WHERE car_number = ?", (car_number,)
        )
        if not driver_rows:
            sys.exit(
                f"No driver with car number {car_number} in the database. "
                f"Scrape an event they ran before recording a national average."
            )
        log.info("no record found, adding record.")
        driver_id = driver_rows[0][0]
        execute_query(
            connection,
            "INSERT INTO class_results VALUES (NULL,?,?,?,0,0,0,0,0,1,0)",
            (event_date, driver_id, car_class),
        )
        execute_query(
            connection,
            "INSERT INTO driver_results VALUES (NULL,?,?,NULL,0,0,0,1,0)",
            (event_date, driver_id),
        )
        generate_points(connection)
        update_average_points(connection, driver_id, car_class)
        return

    log.info("Records Found, updating average")
    driver_id = rows[0][1]
    execute_query(
        connection,
        "UPDATE class_results SET national = 1 "
        "WHERE driver_id = ? AND class = ? AND event_date = ?",
        (driver_id, car_class, event_date),
    )
    execute_query(
        connection,
        "UPDATE driver_results SET national = 1 WHERE driver_id = ? AND event_date = ?",
        (driver_id, event_date),
    )
    update_average_points(connection, driver_id, car_class)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def build_parser():
    argparser = argparse.ArgumentParser(
        description="Calculate SCCA regional club points from Axware results."
    )
    argparser.add_argument(
        "-u",
        "--url",
        help="url to an axware results page, or to a season index listing them "
        "(e.g. https://results.solo2.com/index.php?dir=2026), or a path to a "
        "local html file",
    )
    argparser.add_argument(
        "-a",
        "--average",
        help="car number of a driver who ran a national event; requires -n and -d. "
        "Example: -a 97 -n CS -d 04-03-2022",
    )
    argparser.add_argument(
        "-n", "--name", dest="car_class", help="class name, e.g. CS, PAX, M1"
    )
    argparser.add_argument("-d", "--event_date", help="event date as MM-DD-YYYY")
    argparser.add_argument(
        "-g", "--generate", action="store_true", help="regenerate the points tables"
    )
    argparser.add_argument(
        "-c",
        "--class_points",
        action="store_true",
        help="display class standings; narrow to one class with -n",
    )
    argparser.add_argument(
        "-o",
        "--output",
        choices=("text", "csv"),
        default="text",
        help="output format (default: text)",
    )
    argparser.add_argument("-f", "--file", help="write output to this file")
    argparser.add_argument(
        "--driver", action="store_true", help="display driver (PAX) standings"
    )
    argparser.add_argument(
        "--season",
        help="season (year) to work on. Usually inferred from the results being "
        "scraped or from --event_date; needed only when printing standings for "
        "a season other than the most recent one.",
    )
    argparser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"path to the config file (default: {DEFAULT_CONFIG})",
    )
    argparser.add_argument("--debug", action="store_true", help="enable debug logging")
    return argparser


def main(argv=None):
    argparser = build_parser()
    args = argparser.parse_args(argv)
    setup_logging(args.debug)

    club = load_config(args.config)
    warn_about_legacy_database(club)

    # Points are annual, so every command works on exactly one season. Which
    # season is implied by the inputs where possible, so the common cases need
    # no --season flag at all.
    pending = None
    queued = []
    implied = set()
    if args.url:
        soup = load_soup(args.url)
        queued = result_links(soup, args.url)
        if queued:
            # A season index: take the season from the linked filenames so the
            # database can be opened before anything is fetched.
            log.info("season index: %s result pages", len(queued))
            for url in queued:
                match = DATE_PATTERN.search(os.path.basename(url))
                if match:
                    implied.add(season_year(match.group()))
        else:
            pending = read_results(args.url, soup)
            implied.add(season_year(pending[1]))
    if args.average and args.event_date:
        implied.add(season_year(args.event_date))
    if args.season:
        implied.add(args.season)
    if len(implied) > 1:
        argparser.error(
            f"these inputs span {', '.join(sorted(implied))}. Points are annual, "
            f"so run one season at a time."
        )
    season = implied.pop() if implied else default_season(club)
    log.debug("season: %s", season)
    connection = db_init(database_name(club, season))

    if pending is not None:
        parse_results(connection, *pending)

    failed = 0
    for index, url in enumerate(queued, start=1):
        log.info("[%s/%s] %s", index, len(queued), url)
        try:
            parse_results(connection, *read_results(url))
        except Exception as exc:  # one bad page should not lose the rest
            failed += 1
            log.warning("could not read %s: %s", url, exc)
    if failed:
        log.warning("%s of %s pages failed; re-running is safe", failed, len(queued))

    if args.average:
        if not args.car_class or not args.event_date:
            argparser.error(
                "--average requires --name (class) and --event_date. "
                "Example: -a 97 -n CS -d 04-03-2022"
            )
        if not DATE_PATTERN.fullmatch(args.event_date):
            argparser.error(f"--event_date must be MM-DD-YYYY, got {args.event_date!r}")
        try:
            car_number = int(args.average)
        except ValueError:
            argparser.error(f"--average must be a car number, got {args.average!r}")
        record_national_average(
            connection, car_number, args.car_class.upper(), args.event_date
        )

    # Standings are always printed from freshly generated tables, so an explicit
    # -g alongside -c/--driver would otherwise regenerate twice.
    if args.generate or args.class_points or args.driver:
        generate_points(connection)

    if not (args.class_points or args.driver):
        return

    handle = open(args.file, "w", newline="") if args.file else sys.stdout
    try:
        if args.class_points:
            if args.car_class:
                car_classes = [args.car_class.upper()]
            else:
                rows = execute_read_query(
                    connection,
                    "SELECT DISTINCT class FROM class_points ORDER BY class ASC",
                )
                car_classes = [row[0] for row in rows]
            print_class_standings(connection, car_classes, args.output, handle)
        if args.driver:
            print_driver_standings(connection, args.output, handle)
    finally:
        if handle is not sys.stdout:
            handle.close()


if __name__ == "__main__":
    main()
