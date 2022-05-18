def points_card(number):
    if number < 1000:
        return True
    return False


def calc_points(fastest, driver):
    points_scored = (fastest / driver) * 100
    if points_scored < 70:
        points_scored = 70
    return round(points_scored, 3)


def calc_drops(events):
    return int(events / 3)
