"""Season rules: drops and the participation threshold.

Drops are derived from the number of events HELD, not the number a driver ran.
A driver who runs a handful of events keeps all of them, because the season's
drops are absorbed by the events they missed.
"""

import pytest

import clubpoints


def seed(connection, car_number, name, points_by_date, car_class="CS"):
    clubpoints.execute_query(
        connection, "INSERT INTO drivers VALUES (NULL,?,?)", (name, car_number)
    )
    driver_id = clubpoints.execute_read_query(
        connection, "SELECT id FROM drivers WHERE car_number = ?", (car_number,)
    )[0][0]
    for date, points in points_by_date.items():
        clubpoints.execute_query(
            connection,
            "INSERT INTO class_results VALUES (NULL,?,?,?,1,40.0,?,0,0,0,0)",
            (date, driver_id, car_class, points),
        )
        clubpoints.execute_query(
            connection,
            "INSERT INTO driver_results VALUES (NULL,?,?,?,1,40.0,?,0,0)",
            (date, driver_id, car_class, points),
        )
    return driver_id


def season(n):
    return [f"{m:02d}-01-2026" for m in range(1, n + 1)]


class TestDrops:
    def test_running_every_event_drops_the_lowest_scores(self, sdr_db):
        # SDR: 12 events -> 12 // 3 = 4 drops.
        dates = season(12)
        scores = [100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89]
        driver_id = seed(sdr_db, 97, "Full Season", dict(zip(dates, scores)))
        clubpoints.generate_points(sdr_db)
        assert clubpoints.total_class_points(sdr_db, driver_id, "CS") == sum(
            sorted(scores, reverse=True)[:8]
        )

    def test_running_a_few_events_keeps_all_of_them(self, sdr_db):
        # Ran 3 of 12: the 4 drops are absorbed by the 9 missed events.
        dates = season(12)
        seed(sdr_db, 1, "Full Season", dict.fromkeys(dates, 50))
        driver_id = seed(sdr_db, 97, "Part Timer", dict(zip(dates[:3], [80, 70, 60])))
        clubpoints.generate_points(sdr_db)
        assert clubpoints.total_class_points(sdr_db, driver_id, "CS") == 210

    def test_partial_season_loses_only_the_overflow(self, sdr_db):
        # Ran 10 of 12: 4 drops, 2 absorbed by absences, 2 real scores lost.
        dates = season(12)
        seed(sdr_db, 1, "Full Season", dict.fromkeys(dates, 50))
        scores = [100, 99, 98, 97, 96, 95, 94, 93, 92, 91]
        driver_id = seed(sdr_db, 97, "Mostly There", dict(zip(dates[:10], scores)))
        clubpoints.generate_points(sdr_db)
        assert clubpoints.total_class_points(sdr_db, driver_id, "CS") == sum(
            sorted(scores, reverse=True)[:8]
        )

    def test_too_few_events_for_any_drop(self, sdr_db):
        dates = season(2)
        driver_id = seed(sdr_db, 97, "Newcomer", dict(zip(dates, [80, 70])))
        clubpoints.generate_points(sdr_db)
        assert clubpoints.total_class_points(sdr_db, driver_id, "CS") == 150


class TestEligibility:
    @pytest.mark.parametrize(
        "attended, held, expected",
        [
            (12, 12, True),
            (8, 12, True),   # exactly two thirds
            (7, 12, False),
            (4, 6, True),
            (3, 6, False),
            (2, 3, True),
            (1, 3, False),
            (0, 0, True),    # nothing held yet
        ],
    )
    def test_two_thirds_of_the_season_is_required(self, sdr_db, attended, held, expected):
        assert clubpoints.is_eligible(attended, held) is expected

    def test_each_region_sets_its_own_threshold(self):
        import calclub
        import sdr

        # Cal Club Supp. Regs 7.6: "at least one half (1/2) of all Championship
        # events held in the season". SDR's two thirds is the maintainer's
        # recollection; their rules page was unreachable to verify it.
        assert calclub.MIN_EVENT_FRACTION == pytest.approx(1 / 2)
        assert sdr.MIN_EVENT_FRACTION == pytest.approx(2 / 3)

    @pytest.mark.parametrize(
        "attended, held, expected",
        [(6, 12, True), (5, 12, False), (3, 6, True), (2, 6, False), (1, 2, True)],
    )
    def test_calclub_needs_half_the_season(self, attended, held, expected):
        clubpoints.load_region("calclub")
        try:
            assert clubpoints.is_eligible(attended, held) is expected
        finally:
            clubpoints.load_region("sdr")

    def test_missed_events_do_not_count_as_attendance(self, sdr_db):
        dates = season(6)
        seed(sdr_db, 1, "Full Season", dict.fromkeys(dates, 50))
        driver_id = seed(sdr_db, 97, "Part Timer", dict(zip(dates[:2], [80, 70])))
        clubpoints.generate_points(sdr_db)
        # generate_points fills in placeholder rows for the 4 missed events.
        assert clubpoints.events_attended(sdr_db, driver_id, "CS") == 2
        assert clubpoints.is_eligible(2, 6) is False

    def test_ineligible_drivers_are_marked_not_placed(self, sdr_db):
        import io

        dates = season(6)
        seed(sdr_db, 1, "Full Season", dict.fromkeys(dates, 50))
        seed(sdr_db, 97, "Part Timer", dict(zip(dates[:2], [100, 100])))
        clubpoints.generate_points(sdr_db)
        out = io.StringIO()
        clubpoints.print_class_standings(sdr_db, ["CS"], "text", out)
        body = [l for l in out.getvalue().splitlines() if "Part Timer" in l]
        assert body and body[0].startswith(clubpoints.INELIGIBLE_MARK)
        assert "eligible for end-of-year trophies" in out.getvalue()


class TestStartingVersusNotStarting:
    """A DNF started and scores the region's award; a DNS never started."""

    def test_dnf_scores_the_region_award(self, sdr_db, fixture_path):
        clubpoints.scrape(sdr_db, fixture_path("class_results.html"))
        row = clubpoints.execute_read_query(
            sdr_db,
            "SELECT points FROM class_results JOIN drivers "
            "ON drivers.id = class_results.driver_id WHERE car_number = ?",
            (61,),
        )
        assert row[0][0] == clubpoints.dnf_points() == 70

    def test_dns_records_nothing_at_all(self, sdr_db, fixture_path):
        clubpoints.scrape(sdr_db, fixture_path("class_results.html"))
        cars = [
            r[0]
            for r in clubpoints.execute_read_query(
                sdr_db, "SELECT car_number FROM drivers"
            )
        ]
        assert 62 not in cars

    def test_award_follows_the_configured_region(self, sdr_db):
        import calclub
        import sdr

        clubpoints.load_region("sdr")
        assert clubpoints.dnf_points() == sdr.DNF_POINTS
        clubpoints.load_region("calclub")
        assert clubpoints.dnf_points() == calclub.DNF_POINTS
        clubpoints.load_region("sdr")

    def test_missing_region_setting_falls_back(self, sdr_db):
        import sdr

        saved = sdr.DNF_POINTS
        del sdr.DNF_POINTS
        try:
            assert clubpoints.dnf_points() == clubpoints.DEFAULT_DNF_POINTS
        finally:
            sdr.DNF_POINTS = saved
