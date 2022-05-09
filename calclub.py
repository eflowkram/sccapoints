def points_card(number):
    n = number
    if n >= 600 and n < 700:
        return False
    return True


def calc_points(fastest, driver):
    points_scored = 100 - 400 * ((driver - fastest) / fastest)
    if points_scored < 0:
        points_scored = 0
    return round(points_scored, 3)


def calc_drops(events):
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
