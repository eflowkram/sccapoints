"""San Diego Region (SDR) scoring rules.

Points are a percentage of the class winner's time, floored at 70.
"""

# Cars numbered 1000 and up are non-points entries (fun runs, guests).
MAX_POINTS_CAR_NUMBER = 1000

# Minimum points awarded for a scored run. Start one run and you score at least
# this, however the run goes -- turning up and taking the green is worth points.
MIN_POINTS = 70

# Awarded when a driver starts but records no clean time. Same as the floor: a
# DNF is still a start. (A DNS records nothing at all -- they never started.)
DNF_POINTS = MIN_POINTS

# One drop for every this many events attended.
EVENTS_PER_DROP = 3


# Share of the season's events a driver must run to score points and be eligible
# for end-of-year trophies. UNVERIFIED: from the maintainer's recollection, not
# from SDR's published supplemental rules (sdrscca.com/solorules/), which were
# unreachable when this was written. Cal Club's equivalent is one half.
MIN_EVENT_FRACTION = 2 / 3


def points_card(number):
    return number < MAX_POINTS_CAR_NUMBER


def calc_points(fastest, driver):
    # A missing or invalid winner time means the run cannot be scored on a
    # curve; fall back to the floor rather than dividing by zero.
    if fastest <= 0 or driver <= 0:
        return float(MIN_POINTS)
    points_scored = (fastest / driver) * 100
    return round(max(points_scored, MIN_POINTS), 3)


def calc_drops(events):
    return events // EVENTS_PER_DROP
