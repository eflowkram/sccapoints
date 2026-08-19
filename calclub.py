"""Cal Club scoring rules.

Points fall off from 100 at four times the percentage a driver is off the class
winner's time, floored at 0.

Section numbers refer to the Cal Club Autocross Supplementary Regulations (2024).
The scoring formula is 7.3; drops are Appendix D.
"""

# Cars numbered 600-699 are non-points entries.
NON_POINTS_CAR_RANGE = range(600, 700)

# Minimum points awarded for a scored run.
MIN_POINTS = 0

# Supp. Regs 7.3.1 (Zero Points): "Negative points, did not run (DNR), did not
# finish (DNF) will be considered as zero points for that event." So a DNF scores
# the floor, same as any negative result -- unlike SDR, where turning up is worth
# 70 whatever happens.
DNF_POINTS = MIN_POINTS

# Points awarded to the class winner.
MAX_POINTS = 100

# How steeply points fall off per unit of percentage off the winner's time.
FALLOFF = 400

# Supp. Regs Appendix D (Number of Drops Index):
#   1-3 events -> 0 drops      11-13 -> 3
#   4-6        -> 1            14-16 -> 4
#   7-10       -> 2            17+   -> 5
# Index N-1 holds the threshold for drop N, so the count is simply "how many
# thresholds are met".
DROP_THRESHOLDS = (4, 7, 11, 14, 17)


# Supp. Regs 7.6: "A competitor shall compete in at least one half (1/2) of all
# Championship events held in the season to be eligible for year-end awards."
# An event a driver qualifies for an average at counts as attended.
MIN_EVENT_FRACTION = 1 / 2


def points_card(number):
    return number not in NON_POINTS_CAR_RANGE


def calc_points(fastest, driver):
    # A missing or invalid winner time means the run cannot be scored on a
    # curve; fall back to the floor rather than dividing by zero.
    if fastest <= 0 or driver <= 0:
        return float(MIN_POINTS)
    points_scored = MAX_POINTS - FALLOFF * ((driver - fastest) / fastest)
    return round(max(points_scored, MIN_POINTS), 3)


def calc_drops(events):
    return sum(1 for threshold in DROP_THRESHOLDS if events >= threshold)
