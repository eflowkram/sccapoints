"""Scoring rules for each region.

These are pure functions, so they are the cheapest place to lock in behaviour.
"""

import pytest

import calclub
import sdr


class TestSdr:
    @pytest.mark.parametrize(
        "number, expected", [(1, True), (97, True), (999, True), (1000, False), (1500, False)]
    )
    def test_points_card(self, number, expected):
        assert sdr.points_card(number) is expected

    def test_winner_scores_100(self):
        assert sdr.calc_points(49.100, 49.100) == 100.0

    def test_slower_driver_scores_a_percentage_of_the_winner(self):
        assert sdr.calc_points(50.000, 55.000) == pytest.approx(90.909, abs=0.001)

    def test_points_are_floored_at_70(self):
        assert sdr.calc_points(50.000, 500.000) == 70

    @pytest.mark.parametrize("fastest, driver", [(0.0, 45.0), (45.0, 0.0), (0.0, 0.0)])
    def test_missing_times_do_not_divide_by_zero(self, fastest, driver):
        assert sdr.calc_points(fastest, driver) == 70

    @pytest.mark.parametrize(
        "events, expected", [(0, 0), (2, 0), (3, 1), (5, 1), (6, 2), (12, 4)]
    )
    def test_calc_drops(self, events, expected):
        assert sdr.calc_drops(events) == expected


class TestCalclub:
    @pytest.mark.parametrize(
        "number, expected",
        [(1, True), (599, True), (600, False), (699, False), (700, True)],
    )
    def test_points_card(self, number, expected):
        assert calclub.points_card(number) is expected

    def test_winner_scores_100(self):
        assert calclub.calc_points(49.100, 49.100) == 100.0

    def test_points_fall_off_at_four_times_the_percentage_off(self):
        # 10% off the winner's time costs 40 points.
        assert calclub.calc_points(50.000, 55.000) == pytest.approx(60.0, abs=0.001)

    def test_points_are_floored_at_zero(self):
        assert calclub.calc_points(50.000, 500.000) == 0

    @pytest.mark.parametrize("fastest, driver", [(0.0, 45.0), (45.0, 0.0), (0.0, 0.0)])
    def test_missing_times_do_not_divide_by_zero(self, fastest, driver):
        # Regression: this raised ZeroDivisionError when a class winner DNF'd.
        assert calclub.calc_points(fastest, driver) == 0

    @pytest.mark.parametrize(
        "events, expected",
        # Supp. Regs Appendix D, Number of Drops Index, verbatim:
        #   1 to 3 -> 0, 4 to 6 -> 1, 7 to 10 -> 2,
        #   11 to 13 -> 3, 14 to 16 -> 4, 17 and up -> 5
        [(1, 0), (3, 0), (4, 1), (6, 1), (7, 2), (10, 2),
         (11, 3), (13, 3), (14, 4), (16, 4), (17, 5), (50, 5)],
    )
    def test_calc_drops_matches_appendix_d(self, events, expected):
        assert calclub.calc_drops(events) == expected

    def test_calc_drops_matches_the_original_ladder(self):
        def original(events):
            if events < 4:
                return 0
            elif events < 7:
                return 1
            elif events < 11:
                return 2
            elif events < 14:
                return 3
            elif events < 17:
                return 4
            else:
                return 5

        assert [calclub.calc_drops(n) for n in range(60)] == [
            original(n) for n in range(60)
        ]


class TestParticipationAward:
    """SDR: start one run and you score at least 70, however it goes."""

    def test_sdr_floor_is_the_participation_award(self):
        import sdr

        assert sdr.DNF_POINTS == sdr.MIN_POINTS == 70

    def test_sdr_a_terrible_run_still_scores_the_floor(self):
        import sdr

        # Ten times the winner's time, but they started and finished a run.
        assert sdr.calc_points(40.0, 400.0) == 70

    def test_calclub_scores_a_dnf_as_zero(self):
        import calclub

        # Supp. Regs 7.3.1: "Negative points, did not run (DNR), did not finish
        # (DNF) will be considered as zero points for that event."
        assert calclub.MIN_POINTS == 0
        assert calclub.DNF_POINTS == 0

    def test_the_two_regions_disagree_about_dnfs(self):
        import calclub
        import sdr

        assert sdr.DNF_POINTS != calclub.DNF_POINTS
