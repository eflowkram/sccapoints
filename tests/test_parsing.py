"""HTML parsing helpers and the pure points bookkeeping."""

import pytest
from bs4 import BeautifulSoup

import clubpoints


def soup_for(path):
    with open(path) as handle:
        return BeautifulSoup(handle, "html.parser")


class TestConeAndDnfCounting:
    def test_counts_single_digit_cones(self):
        row = ["1", "", "97", "MARK", "50.000+1", "49.000+2", "49.000", "-"]
        assert clubpoints.get_cone_dnf(row, [4, 5]) == (3, 0)

    def test_counts_double_digit_cones(self):
        # Regression: the old "\+\d{1}" pattern read "+10" as a single cone.
        row = ["1", "", "97", "MARK", "50.000+10", "49.000", "49.000", "-"]
        assert clubpoints.get_cone_dnf(row, [4, 5]) == (10, 0)

    def test_counts_dnf_runs(self):
        row = ["1", "", "97", "MARK", "50.000+DNF", "49.000+DNF", "DNF", "-"]
        assert clubpoints.get_cone_dnf(row, [4, 5]) == (0, 2)

    def test_only_counts_the_given_run_columns(self):
        # The total column repeats the winning run; counting it would double up.
        row = ["1", "", "97", "MARK", "50.000", "49.000+3", "49.000+3", "-"]
        assert clubpoints.get_cone_dnf(row, [4, 5]) == (3, 0)

    def test_legacy_scan_ignores_the_last_two_columns(self):
        row = ["1", "", "97", "MARK", "50.000", "49.000+3", "49.000+9", "-"]
        assert clubpoints.get_cone_dnf(row) == (3, 0)

    def test_out_of_range_run_columns_are_skipped(self):
        assert clubpoints.get_cone_dnf(["1", "50.000+2"], [1, 9]) == (2, 0)


class TestClassColumns:
    JANUARY = ["CS - 'C Street'Total Entries: 4", "Car Color",
               "Run 1", "Run 2", "Run 3", "Run 4", "Total", "Diff."]
    AUGUST = ["CS - 'C Street'Total Entries: 4", "S/T", "Factor",
              "Run 1..", "Run 2..", "Run 3..", "Run 4..", "Total"]

    def test_january_layout_puts_total_second_from_last(self):
        total, runs = clubpoints.class_columns(self.JANUARY, 12)
        assert total == 10
        assert runs == [6, 7, 8, 9]

    def test_august_layout_puts_total_last(self):
        # Regression: a fixed "second to last" rule read Run 4 as the best time.
        total, runs = clubpoints.class_columns(self.AUGUST, 12)
        assert total == 11
        assert runs == [7, 8, 9, 10]

    def test_unknown_layout_falls_back_without_disabling_cone_counting(self):
        total, runs = clubpoints.class_columns(["CS", "A", "B", "Best"], 9)
        assert total == 7
        assert runs is None


class TestPaxColumns:
    def test_nine_column_layout(self):
        header = ["Pax Pos.", "Class", "#", "Driver", "Car Model",
                  "Factor", "Pax Time", "Diff.", "From 1st"]
        assert clubpoints.pax_columns(header) == {
            "class": 1, "number": 2, "driver": 3, "time": 6
        }

    def test_eleven_column_layout_shifts_everything_right(self):
        # Regression: reading the car number from index 2 hit the class name
        # ("PAXSS") on these sheets and raised ValueError.
        header = ["Pax Pos.", "Pos.", "Class", "#", "Driver", "Car Model",
                  "Total", "Factor", "Pax Time", "Diff.", "From 1st"]
        assert clubpoints.pax_columns(header) == {
            "class": 2, "number": 3, "driver": 4, "time": 8
        }

    def test_unrecognised_header_falls_back_to_legacy_positions(self):
        header = ["Pos", "Class", "No", "Name", "T1", "T2", "T3", "T4", "Best"]
        assert clubpoints.pax_columns(header) == {
            "class": 1, "number": 2, "driver": 3, "time": 6
        }


class TestGetCarClass:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("PAXL2", "PAXL"),
            ("PAX3", "PAX"),
            ("PAX", "PAX"),
            ("HST1A", "HST1"),
            ("HST2B", "HST2"),
            ("SP1", "SP"),
            ("XS4", "XS"),
            ("P2", "P"),
            ("CS", "CS"),
            ("STS", "STS"),
        ],
    )
    def test_collapses_combined_classes(self, raw, expected):
        assert clubpoints.get_car_class(raw) == expected

    def test_paxl_wins_over_pax(self):
        # Ordering matters: PAXL must be checked before its PAX prefix.
        assert clubpoints.get_car_class("PAXL") == "PAXL"


class TestHeaderParsing:
    def test_reads_the_event_date(self, fixture_path):
        soup = soup_for(fixture_path("class_results.html"))
        assert clubpoints.get_event_date(soup.find_all("table")[0]) == "04-10-2022"

    def test_falls_back_to_the_filename_when_the_header_has_no_date(self):
        # Some events are titled by name only ("#1 - JAnuary Championship").
        soup = BeautifulSoup(
            "<table><tr><td>Series - #1 - JAnuary Championship</td></tr></table>",
            "html.parser",
        )
        table = soup.find_all("table")[0]
        assert clubpoints.get_event_date(
            table, source="/results/2026/01-25-2026-Class.htm"
        ) == "01-25-2026"

    def test_exits_when_no_date_can_be_found_anywhere(self):
        soup = BeautifulSoup("<table><tr><td>no date</td></tr></table>", "html.parser")
        with pytest.raises(SystemExit):
            clubpoints.get_event_date(soup.find_all("table")[0], source="results.htm")

    def test_defaults_to_standard_format(self, fixture_path):
        soup = soup_for(fixture_path("class_results.html"))
        assert clubpoints.get_file_format(soup.find_all("table")[0]) == "standard"

    def test_detects_mobile_format(self):
        soup = BeautifulSoup("<table><tr><td>Mobile Results</td></tr></table>", "html.parser")
        assert clubpoints.get_file_format(soup.find_all("table")[0]) == "mobile"


class TestSelectParser:
    def test_four_tables_is_a_class_sheet(self, fixture_path):
        soup = soup_for(fixture_path("class_results.html"))
        assert clubpoints.select_parser(soup) is clubpoints.class_point_parser

    def test_two_tables_is_a_pax_sheet(self, fixture_path):
        soup = soup_for(fixture_path("pax_results.html"))
        assert clubpoints.select_parser(soup) is clubpoints.driver_point_parser

    def test_unrecognized_layout_exits(self):
        soup = BeautifulSoup("<table><tr><td>only one</td></tr></table>", "html.parser")
        with pytest.raises(SystemExit):
            clubpoints.select_parser(soup)


class TestDrops:
    def test_keeps_the_best_scores(self):
        assert clubpoints._points_after_drops([10, 50, 30, 20], drops=2) == 80

    def test_no_drops_keeps_everything(self):
        assert clubpoints._points_after_drops([10, 50, 30], drops=0) == 90

    def test_dropping_everything_is_not_negative_slicing(self):
        # A negative keep-count would silently drop from the wrong end.
        assert clubpoints._points_after_drops([10, 20], drops=5) == 0
