"""Scrape the fixtures into a database and check the standings that come out."""

import io

import pytest

import clubpoints


def scored_class_rows(connection):
    return execute(
        connection,
        "SELECT drivers.car_number, class_results.class, class_results.points, "
        "class_results.cones, class_results.dnf "
        "FROM class_results JOIN drivers ON drivers.id = class_results.driver_id "
        "ORDER BY drivers.car_number",
    )


def execute(connection, sql, params=()):
    return clubpoints.execute_read_query(connection, sql, params)


class TestClassScraping:
    @pytest.fixture(autouse=True)
    def scraped(self, sdr_db, fixture_path):
        clubpoints.scrape(sdr_db, fixture_path("class_results.html"))
        self.db = sdr_db

    def test_records_only_scoring_entries(self):
        cars = [row[0] for row in scored_class_rows(self.db)]
        # 62 ran DNS, 1500 is over SDR's points cutoff, 77 is in class N.
        assert cars == [42, 55, 61, 97]

    def test_winner_scores_100(self):
        points = dict(execute(self.db, "SELECT driver_id, points FROM class_results"))
        winner = execute(
            self.db, "SELECT id FROM drivers WHERE car_number = ?", (97,)
        )[0][0]
        assert points[winner] == 100.0

    def test_scores_are_relative_to_the_class_winner(self):
        row = execute(
            self.db,
            "SELECT points FROM class_results JOIN drivers "
            "ON drivers.id = class_results.driver_id WHERE car_number = ?",
            (42,),
        )
        assert row[0][0] == pytest.approx(98.2, abs=0.001)

    def test_double_digit_cones_are_counted(self):
        row = execute(
            self.db,
            "SELECT cones FROM class_results JOIN drivers "
            "ON drivers.id = class_results.driver_id WHERE car_number = ?",
            (55,),
        )
        assert row[0][0] == 10

    def test_all_runs_dnf_scores_the_dnf_award(self):
        # Axware writes the total as 0.000 when every run DNF'd.
        row = execute(
            self.db,
            "SELECT points, dnf, final_time FROM class_results JOIN drivers "
            "ON drivers.id = class_results.driver_id WHERE car_number = ?",
            (61,),
        )
        points, dnf, final_time = row[0]
        assert points == clubpoints.dnf_points()
        assert dnf == 4
        assert final_time == 0.0

    def test_blank_and_dns_totals_are_skipped(self):
        cars = [row[0] for row in scored_class_rows(self.db)]
        assert 62 not in cars  # total is "DNS"
        assert 63 not in cars  # total is blank

    def test_non_points_classes_are_skipped(self):
        assert execute(
            self.db, "SELECT count(1) FROM class_results WHERE class = 'N'"
        )[0][0] == 0

    def test_rescraping_the_same_event_is_idempotent(self, fixture_path):
        before = execute(self.db, "SELECT count(1) FROM class_results")[0][0]
        clubpoints.scrape(self.db, fixture_path("class_results.html"))
        assert execute(self.db, "SELECT count(1) FROM class_results")[0][0] == before


class TestPaxScraping:
    @pytest.fixture(autouse=True)
    def scraped(self, sdr_db, fixture_path):
        clubpoints.scrape(sdr_db, fixture_path("pax_results.html"))
        self.db = sdr_db

    def test_records_only_scoring_entries(self):
        cars = [
            row[0]
            for row in execute(
                self.db,
                "SELECT car_number FROM driver_results JOIN drivers "
                "ON drivers.id = driver_results.driver_id ORDER BY car_number",
            )
        ]
        # 1500 is over SDR's cutoff, 62 is a DNS.
        assert cars == [42, 61, 97]

    def test_car_number_is_read_from_the_hash_column(self):
        # Regression: the 11-column PAX sheet puts the class where the older
        # 9-column sheet put the car number, which used to raise ValueError.
        names = dict(
            execute(
                self.db,
                "SELECT car_number, driver_name FROM drivers ORDER BY car_number",
            )
        )
        assert names[97] == "Mark W"

    def test_reads_the_indexed_pax_time(self):
        # PAX standings are scored on the indexed time, not the raw run.
        row = execute(
            self.db,
            "SELECT final_time FROM driver_results JOIN drivers "
            "ON drivers.id = driver_results.driver_id WHERE car_number = ?",
            (97,),
        )
        assert row[0][0] == pytest.approx(39.967)

    def test_dnf_scores_the_dnf_award(self):
        row = execute(
            self.db,
            "SELECT points FROM driver_results JOIN drivers "
            "ON drivers.id = driver_results.driver_id WHERE car_number = ?",
            (61,),
        )
        assert row[0][0] == clubpoints.dnf_points()


class TestStandingsOutput:
    @pytest.fixture(autouse=True)
    def scraped(self, sdr_db, fixture_path):
        clubpoints.scrape(sdr_db, fixture_path("class_results.html"))
        clubpoints.scrape(sdr_db, fixture_path("pax_results.html"))
        clubpoints.generate_points(sdr_db)
        self.db = sdr_db

    def test_class_standings_are_ordered_by_points(self):
        out = io.StringIO()
        clubpoints.print_class_standings(self.db, ["CS"], "text", out)
        lines = [line for line in out.getvalue().splitlines() if line.strip()]
        # Header, then the three CS drivers best-first.
        assert "Place" in lines[0]
        assert "Mark W" in lines[1]
        assert "Jane D" in lines[2]

    def test_csv_output_quotes_names_with_commas(self):
        clubpoints.execute_query(
            self.db, "UPDATE drivers SET driver_name = ? WHERE car_number = ?",
            ("Wolfe, Mark", 97),
        )
        out = io.StringIO()
        clubpoints.print_class_standings(self.db, ["CS"], "csv", out)
        assert '"Wolfe, Mark"' in out.getvalue()

    def test_driver_standings_render(self):
        out = io.StringIO()
        clubpoints.print_driver_standings(self.db, "text", out)
        assert "Mark W" in out.getvalue()

    def test_class_points_table_is_populated(self):
        rows = execute(self.db, "SELECT count(1) FROM class_points")
        assert rows[0][0] > 0


class TestNationalAverage:
    @pytest.fixture(autouse=True)
    def scraped(self, sdr_db, fixture_path):
        clubpoints.scrape(sdr_db, fixture_path("class_results.html"))
        self.db = sdr_db

    def test_unknown_car_number_exits_cleanly(self):
        # Regression: this used to raise IndexError on results[0][0].
        with pytest.raises(SystemExit):
            clubpoints.record_national_average(self.db, 9999, "CS", "05-01-2022")

    def test_a_driver_with_no_prior_results_does_not_divide_by_zero(self, sdr_db):
        clubpoints.execute_query(
            sdr_db, "INSERT INTO drivers VALUES (NULL,?,?)", ("Newcomer N", 123)
        )
        driver_id = execute(
            sdr_db, "SELECT id FROM drivers WHERE car_number = ?", (123,)
        )[0][0]
        clubpoints.execute_query(
            sdr_db,
            "INSERT INTO class_results VALUES (NULL,?,?,?,0,0,0,0,0,1,0)",
            ("04-10-2022", driver_id, "XX"),
        )
        # No scored XX results to average; must warn rather than crash.
        clubpoints.update_average_points(sdr_db, driver_id, "XX")
