"""Regression tests against real Axware exports.

See fixtures/real/SOURCE.md. The two events use different column layouts, which
is the whole reason the parsers resolve columns from the header row.
"""

import os

import pytest

import clubpoints

REAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "real")

EVENTS = ["04-03-2022", "01-25-2026", "04-25-2026", "08-16-2026"]

# Points are annual, so anything that loads more than one event has to stay
# inside a single season.
SEASON_2026 = ["01-25-2026", "04-25-2026", "08-16-2026"]


@pytest.fixture
def calclub_db():
    clubpoints.load_region("calclub")
    clubpoints.NON_POINTS = ("X", "TO")
    connection = clubpoints.db_init(":memory:")
    yield connection
    connection.close()


def path(event, kind):
    return os.path.join(REAL, f"{event}-{kind}.htm")


@pytest.mark.parametrize("event", EVENTS)
@pytest.mark.parametrize("kind", ["Class", "PAX"])
def test_every_export_parses(calclub_db, event, kind):
    clubpoints.scrape(calclub_db, path(event, kind))
    table = "class_results" if kind == "Class" else "driver_results"
    count = clubpoints.execute_read_query(
        calclub_db, f"SELECT count(1) FROM {table}"
    )[0][0]
    assert count > 30


@pytest.mark.parametrize("event", EVENTS)
def test_event_date_is_recovered(calclub_db, event):
    # January's header has no date at all; it comes from the filename.
    clubpoints.scrape(calclub_db, path(event, "Class"))
    assert clubpoints.event_dates(calclub_db) == [event]


class TestAugustLayout:
    """Total sits in the last column here, not the second to last."""

    @pytest.fixture(autouse=True)
    def scraped(self, calclub_db):
        clubpoints.scrape(calclub_db, path("08-16-2026", "Class"))
        self.db = calclub_db

    def test_scores_the_total_not_the_final_run(self):
        # Paul R ran 40.015 / 39.840 / 39.466 / 39.892; Total is 39.466.
        # Reading "second to last" gave 39.892, his fourth run.
        row = clubpoints.execute_read_query(
            self.db,
            "SELECT final_time, points FROM class_results JOIN drivers "
            "ON drivers.id = class_results.driver_id "
            "WHERE car_number = ? AND class = ?",
            (57, "CS"),
        )
        assert row[0][0] == pytest.approx(39.466)
        assert row[0][1] == 100.0

    def test_class_winner_scores_full_points(self):
        rows = clubpoints.execute_read_query(
            self.db,
            "SELECT class, max(points) FROM class_results "
            "WHERE class IN ('SS','AS','BS','CS','DS','FS') GROUP BY class",
        )
        assert rows and all(points == 100.0 for _, points in rows)

    def test_non_points_car_numbers_are_excluded(self):
        # Cal Club: 600-699 are non-points entries.
        rows = clubpoints.execute_read_query(
            self.db,
            "SELECT count(1) FROM class_results JOIN drivers "
            "ON drivers.id = class_results.driver_id "
            "WHERE car_number BETWEEN 600 AND 699",
        )
        assert rows[0][0] == 0

    def test_cones_are_counted_from_the_run_columns(self):
        total = clubpoints.execute_read_query(
            self.db, "SELECT sum(cones), sum(dnf) FROM class_results"
        )[0]
        assert total[0] > 0 and total[1] > 0


class TestJanuaryPaxLayout:
    """Eleven columns with an extra `Pos.`, shifting Class/#/Driver right."""

    @pytest.fixture(autouse=True)
    def scraped(self, calclub_db):
        clubpoints.scrape(calclub_db, path("01-25-2026", "PAX"))
        self.db = calclub_db

    def test_car_numbers_are_numeric(self):
        # Regression: reading index 2 hit the class ("PAXSS") and raised
        # ValueError, killing the whole run.
        rows = clubpoints.execute_read_query(
            self.db, "SELECT car_number FROM drivers"
        )
        assert rows and all(isinstance(n, int) for (n,) in rows)

    def test_driver_names_are_names(self):
        rows = clubpoints.execute_read_query(
            self.db, "SELECT driver_name FROM drivers LIMIT 5"
        )
        assert all(" " in name for (name,) in rows)

    def test_indexed_pax_time_is_recorded(self):
        # Sam M: raw total 32.159, factor 0.840, PAX time 32.159.
        row = clubpoints.execute_read_query(
            self.db,
            "SELECT final_time FROM driver_results JOIN drivers "
            "ON drivers.id = driver_results.driver_id WHERE car_number = ?",
            (24,),
        )
        assert row[0][0] == pytest.approx(32.159)


def test_full_season_standings_render(calclub_db):
    import io

    for event in SEASON_2026:
        clubpoints.scrape(calclub_db, path(event, "Class"))
        clubpoints.scrape(calclub_db, path(event, "PAX"))
    clubpoints.generate_points(calclub_db)
    out = io.StringIO()
    clubpoints.print_class_standings(calclub_db, ["CS"], "text", out)
    clubpoints.print_driver_standings(calclub_db, "text", out)
    text = out.getvalue()
    assert "Place" in text and "Points" in text
    # Three events held, so two thirds means at least two.
    assert clubpoints.INELIGIBLE_MARK in text


class TestOldestLayout:
    """2022: 9-wide class header over 13-wide rows, so Total sits at index 12."""

    @pytest.fixture(autouse=True)
    def scraped(self, calclub_db):
        clubpoints.scrape(calclub_db, path("04-03-2022", "Class"))
        self.db = calclub_db

    def test_scores_the_total_column(self):
        # Jeff Spencer ran 66.507 / 65.716 / 66.515+2 / 65.833+1; Total 65.716.
        # A fixed "second to last" rule hit "65.833+1" and raised ValueError,
        # which aborted the whole class file.
        row = clubpoints.execute_read_query(
            self.db,
            "SELECT final_time, points, cones FROM class_results JOIN drivers "
            "ON drivers.id = class_results.driver_id "
            "WHERE car_number = ? AND class = ?",
            (376, "CS"),
        )
        assert row[0][0] == pytest.approx(65.716)
        assert row[0][1] == 100.0
        assert row[0][2] == 3

    def test_the_whole_class_file_is_parsed(self):
        # Previously this aborted partway through on a "+1" in the run column,
        # leaving only 3 rows behind.
        count = clubpoints.execute_read_query(
            self.db, "SELECT count(1) FROM class_results"
        )[0][0]
        assert count > 70
        classes = clubpoints.execute_read_query(
            self.db, "SELECT count(DISTINCT class) FROM class_results"
        )[0][0]
        assert classes > 20

    def test_pax_car_number_column(self, calclub_db):
        # The 2022 PAX sheet is also 11 wide, but with # at index 2 rather
        # than 3 -- same width, different meaning.
        clubpoints.scrape(calclub_db, path("04-03-2022", "PAX"))
        rows = clubpoints.execute_read_query(
            calclub_db, "SELECT car_number FROM drivers"
        )
        assert rows and all(isinstance(n, int) for (n,) in rows)


class TestNonPointsCarsSetTheBenchmark:
    """Cal Club: a 600-series car scores nothing, but its time still curves
    everyone else."""

    def test_class_benchmark_comes_from_the_outright_winner(self, calclub_db):
        clubpoints.scrape(calclub_db, path("04-25-2026", "Class"))
        # CS places 1-3 are cars 602, 613 and 698; the winner ran 42.996.
        # Warren E (231) ran 45.506, which is 76.649 on Cal Club's curve.
        scored = clubpoints.execute_read_query(
            calclub_db,
            "SELECT car_number, points FROM class_results JOIN drivers "
            "ON drivers.id = class_results.driver_id "
            "WHERE class = ? ORDER BY points DESC",
            ("CS",),
        )
        assert [car for car, _ in scored] == [231, 96]
        assert dict(scored)[231] == pytest.approx(76.649, abs=0.001)

    def test_nobody_is_awarded_full_points_when_the_winner_cannot_score(
        self, calclub_db
    ):
        clubpoints.scrape(calclub_db, path("04-25-2026", "Class"))
        best = clubpoints.execute_read_query(
            calclub_db, "SELECT max(points) FROM class_results WHERE class = 'CS'"
        )[0][0]
        assert best < 100.0

    def test_pax_benchmark_comes_from_the_outright_winner(self, calclub_db):
        clubpoints.scrape(calclub_db, path("04-25-2026", "PAX"))
        # Car 647 wins outright at 31.658 but cannot score; Taylour W (199)
        # ran 31.791, which is 98.32 curved against 647.
        rows = clubpoints.execute_read_query(
            calclub_db,
            "SELECT car_number, points FROM driver_results JOIN drivers "
            "ON drivers.id = driver_results.driver_id ORDER BY points DESC LIMIT 1",
        )
        assert rows[0][0] == 199
        assert rows[0][1] == pytest.approx(98.32, abs=0.001)

    def test_non_points_cars_are_absent_from_the_results(self, calclub_db):
        clubpoints.scrape(calclub_db, path("04-25-2026", "PAX"))
        rows = clubpoints.execute_read_query(
            calclub_db,
            "SELECT count(1) FROM drivers WHERE car_number BETWEEN 600 AND 699",
        )
        assert rows[0][0] == 0


class TestNationalAverageOnRealResults:
    def test_missed_event_gets_a_placeholder_holding_the_average(self, calclub_db):
        clubpoints.scrape(calclub_db, path("04-03-2022", "Class"))
        clubpoints.scrape(calclub_db, path("04-03-2022", "PAX"))
        # Jeff Spencer won CS that day with 100 points.
        clubpoints.record_national_average(calclub_db, 376, "CS", "05-01-2022")
        rows = clubpoints.execute_read_query(
            calclub_db,
            "SELECT event_date, points, national FROM class_results "
            "JOIN drivers ON drivers.id = class_results.driver_id "
            "WHERE car_number = ? AND class = ? ORDER BY event_date",
            (376, "CS"),
        )
        placeholder = [r for r in rows if r[2] == 1]
        assert len(placeholder) == 1
        assert placeholder[0][0] == "05-01-2022"
        assert placeholder[0][1] == pytest.approx(100.0)


class TestAverageIsRecalculated:
    """The national-event average is the mean of every event the driver ran,
    and it is recomputed from scratch whenever points are generated."""

    def test_average_follows_the_driver_as_events_are_added(self, calclub_db):
        # Hector G (65) runs ST at all three 2026 events with different scores.
        clubpoints.scrape(calclub_db, path("01-25-2026", "Class"))
        clubpoints.scrape(calclub_db, path("01-25-2026", "PAX"))
        clubpoints.record_national_average(calclub_db, 65, "ST", "02-28-2026")

        def placeholder():
            return clubpoints.execute_read_query(
                calclub_db,
                "SELECT points FROM class_results JOIN drivers "
                "ON drivers.id = class_results.driver_id "
                "WHERE car_number = ? AND class = ? AND national = 1",
                (65, "ST"),
            )[0][0]

        def attended():
            return [
                row[0]
                for row in clubpoints.execute_read_query(
                    calclub_db,
                    "SELECT points FROM class_results JOIN drivers "
                    "ON drivers.id = class_results.driver_id "
                    "WHERE car_number = ? AND class = ? "
                    "AND national = 0 AND missed = 0",
                    (65, "ST"),
                )
            ]

        first = attended()
        assert placeholder() == pytest.approx(round(sum(first) / len(first), 3))

        clubpoints.scrape(calclub_db, path("04-25-2026", "Class"))
        clubpoints.generate_points(calclub_db)

        now = attended()
        assert len(now) > len(first)
        assert placeholder() == pytest.approx(round(sum(now) / len(now), 3))

    def test_placeholders_for_missed_events_are_excluded_from_the_average(
        self, calclub_db
    ):
        clubpoints.scrape(calclub_db, path("01-25-2026", "Class"))
        clubpoints.scrape(calclub_db, path("04-25-2026", "Class"))
        # Ken S (39) won CS in January and did not run CS in April.
        clubpoints.record_national_average(calclub_db, 39, "CS", "08-16-2026")
        clubpoints.generate_points(calclub_db)
        rows = clubpoints.execute_read_query(
            calclub_db,
            "SELECT points, national, missed FROM class_results JOIN drivers "
            "ON drivers.id = class_results.driver_id "
            "WHERE car_number = ? AND class = ?",
            (39, "CS"),
        )
        assert any(m == 1 for _, _, m in rows)
        national = [p for p, n, _ in rows if n == 1][0]
        # Only the January result counts; the April placeholder must not drag
        # the average down to 50.
        assert national == pytest.approx(100.0)


class TestSeasonsAreSeparate:
    """January starts at zero: 2022 points never mix with 2026."""

    def test_season_year_comes_from_the_event_date(self):
        assert clubpoints.season_year("04-03-2022") == "2022"
        assert clubpoints.season_year("01-25-2026") == "2026"

    def test_database_is_named_per_season(self):
        assert clubpoints.database_name("calclub", "2026") == "calclub_2026_points.db"

    def test_adding_another_season_is_refused(self, calclub_db):
        clubpoints.scrape(calclub_db, path("01-25-2026", "Class"))
        with pytest.raises(SystemExit) as exc:
            clubpoints.scrape(calclub_db, path("04-03-2022", "Class"))
        assert "annual" in str(exc.value)

    def test_same_season_events_are_fine(self, calclub_db):
        for event in SEASON_2026:
            clubpoints.scrape(calclub_db, path(event, "Class"))
        years = {
            clubpoints.season_year(d) for d in clubpoints.event_dates(calclub_db)
        }
        assert years == {"2026"}


class TestNonCompetitiveEntries:
    """X (eXtra runs) and TO (Time Only) are not competing, so they do not score."""

    def test_extra_run_entries_are_skipped_by_class(self, calclub_db):
        clubpoints.scrape(calclub_db, path("04-03-2022", "Class"))
        assert clubpoints.execute_read_query(
            calclub_db, "SELECT count(1) FROM class_results WHERE class = 'X'"
        )[0][0] == 0

    def test_x_suffixed_car_numbers_are_skipped_even_without_the_class(
        self, calclub_db
    ):
        # The 2022 X class carries numbers like "681X". Independently of the
        # non_points setting, those must not be parsed as car 681.
        clubpoints.NON_POINTS = ("TO",)
        clubpoints.scrape(calclub_db, path("04-03-2022", "Class"))
        cars = [
            row[0]
            for row in clubpoints.execute_read_query(
                calclub_db, "SELECT car_number FROM drivers"
            )
        ]
        assert 681 not in cars and 617 not in cars and 606 not in cars
        # ...and the file still parses in full rather than dying on int("681X").
        assert clubpoints.execute_read_query(
            calclub_db, "SELECT count(1) FROM class_results"
        )[0][0] > 70

    def test_time_only_entries_are_skipped(self, calclub_db):
        clubpoints.scrape(calclub_db, path("04-25-2026", "Class"))
        assert clubpoints.execute_read_query(
            calclub_db, "SELECT count(1) FROM class_results WHERE class = 'TO'"
        )[0][0] == 0

    def test_a_time_only_driver_still_scores_in_their_real_class(self, calclub_db):
        # Running Time Only at one event does not remove you from the standings;
        # it just does not add anything.
        clubpoints.scrape(calclub_db, path("08-16-2026", "Class"))
        rows = clubpoints.execute_read_query(
            calclub_db,
            "SELECT DISTINCT class FROM class_results ORDER BY class",
        )
        classes = [r[0] for r in rows]
        assert "TO" not in classes and "X" not in classes
        assert "CS" in classes
