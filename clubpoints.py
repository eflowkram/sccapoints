#!/usr/bin/env python3

from configparser import ConfigParser
import requests
import importlib
from bs4 import BeautifulSoup
import argparse
import sys
import os
import sqlite3
import re
from sqlite3 import Error


config = ConfigParser()
config.read("config.ini")
# get the club name and use it to pull in modules
club = config.get("region", "club")
CT = int(config.get("region", "CT"))
non_points = config.get("region", "non_points")
non_points = non_points.split(",")

# importlib will let you use import_module, but it is imported into it's
# own namespace, so we move the functions to global NS.
ns = importlib.import_module(club)
calc_points = ns.calc_points

points_card = ns.points_card
calc_drops = ns.calc_drops

database_name = f"{club}_points.db"

DEBUG = False

class_results_table = """
CREATE TABLE class_results (
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
"""

drivers_table = """
CREATE TABLE drivers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_name varchar(50),
    car_number int(10) unique
);
"""

points_table = """
CREATE TABLE class_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id int(10),
    class varchar(10),
    points decimal(10,3),
    cones int(10),
    dnf int(10),
    unique (driver_id,class)
);
"""

driver_results_table = """
CREATE TABLE driver_results (
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
"""

driver_points_table = """
CREATE TABLE driver_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id int(10),
    points decimal(10,3),
    unique (driver_id)
);
"""


def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        if DEBUG:
            print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        if DEBUG:
            print("Query executed successfully")
        return cursor.lastrowid
    except Error as e:
        print(f"The error '{e}' occurred")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


def event_dates():
    event_d = []
    sql = f"select distinct event_date from class_results"
    results = execute_read_query(db_conn, sql)
    for d in results:
        event_d.append(d[0])
    return event_d


def update_average_points(driver_id, car_class):
    sum_points = float()
    sql = (
        f"SELECT "
        f"count(1) "
        f"FROM "
        f"class_results "
        f"WHERE "
        f"driver_id = {driver_id} and national = 1"
    )
    results = execute_read_query(db_conn, sql)
    if DEBUG:
        print(results)
    if results[0][0] == 0:
        return
    sql = (
        f"SELECT "
        f"points "
        f"FROM "
        f"class_results "
        f"WHERE class = '{car_class}' and driver_id = '{driver_id}' and national = 0 and missed = 0"
    )
    points = execute_read_query(db_conn, sql)
    points_count = len(points)
    for n in points:
        sum_points += n[0]
    if DEBUG:
        print(
            f"driver_id: {driver_id} points_count: {points_count} sum_points: {sum_points}"
        )
    avg_points = sum_points / points_count
    avg_points = round(avg_points, 3)
    sql = (
        f"UPDATE "
        f"class_results "
        f"SET points = {avg_points} "
        f"WHERE class = '{car_class}' and driver_id = '{driver_id}' and national = 1"
    )
    execute_query(db_conn, sql)
    sum_driver_points = float()
    sql = (
        f"SELECT "
        f"points "
        f"FROM "
        f"driver_results "
        f"WHERE "
        f"driver_id = '{driver_id}' and national = 0 and missed = 0"
    )
    driver_points = execute_read_query(db_conn, sql)
    if DEBUG:
        print(driver_points)
    points_count = len(driver_points)
    for n in driver_points:
        sum_driver_points += n[0]
    if DEBUG:
        print(
            f"driver_id: {driver_id} points_count: {points_count} sum_points: {sum_driver_points}"
        )
    avg_points = sum_driver_points / points_count
    avg_points = round(avg_points, 3)
    sql = f"UPDATE driver_results set points = {avg_points} where driver_id = '{driver_id}' and national = 1"
    execute_query(db_conn, sql)
    return


def total_class_points(driver_id, car_class):
    rp = []
    sql = f"select points from class_results where driver_id={driver_id} and class='{car_class}'"
    class_points_results = execute_read_query(db_conn, sql)
    drops = calc_drops(len(class_points_results))
    if DEBUG:
        print(f"number of events: {len(class_points_results)} drops: {drops}")
    count = len(class_points_results) - drops
    for p in class_points_results:
        rp.append(p[0])
    rp.sort(reverse=True)
    rp = rp[:count]
    tp = round(sum(rp), 3)
    if DEBUG:
        print(f"total points: {tp}")
    return tp


def total_driver_points(driver_id):
    rp = []
    sql = f"select points from driver_results where driver_id={driver_id}"
    driver_points_results = execute_read_query(db_conn, sql)
    drops = calc_drops(len(driver_points_results))
    count = len(driver_points_results) - drops
    for p in driver_points_results:
        rp.append(p[0])
    rp.sort(reverse=True)
    rp = rp[:count]
    dp = round(sum(rp), 3)
    if DEBUG:
        print(
            f"driver_id: {driver_id} number of events: {len(driver_points_results)} points: {dp} drops: {drops}"
        )
    return dp


def list_to_string(s):
    string = ""
    for element in s:
        string += element
    return string


def table_data(table_handle):
    return [
        [cell.text.strip() for cell in row.find_all(["th", "td"])]
        for row in table_handle.find_all("tr")
    ]


def get_event_date(table_handle):
    date_mask = "\\d{2}-\\d{2}-\\d{4}"
    header_data = table_data(table_handle)
    event_date = None
    for line in header_data:
        result = re.search(date_mask, str(line))
        if result is not None:
            event_date = result.group()
    if event_date is None:
        sys.exit("Event Date not found!")
    return event_date


def get_cone_dnf(table_row):
    tr = table_row
    # don't count last 2 columns
    column_count = len(tr) - 2
    cones, dnf = 0, 0
    pc = "\\+\\d{1}"
    pd = "\\+DNF"
    for i in tr[:column_count]:
        result_c = re.search(pc, str(i))
        result_d = re.search(pd, str(i))
        if result_c is not None:
            cones = cones + int(result_c.group())
        if result_d is not None:
            dnf += 1
    return cones, dnf


def db_init():
    global db_conn
    if not os.path.isfile(database_name):
        db_conn = create_connection(database_name)
        for table in (
            class_results_table,
            drivers_table,
            points_table,
            driver_results_table,
            driver_points_table,
        ):
            execute_query(db_conn, table)
    else:
        db_conn = create_connection(database_name)


def class_header_text(event_c):
    e = ""
    for i in range(1, event_c + 1):
        e += f"{'Event '}{i : <4}"
    h = (
        f"\n{'Place' : <10}{'Driver' : <25}{'Car' : <8}{'Class' : <9}"
        + e
        + f"{'Points' : <10}{'Cones' : <8}{'DNF' : <5}"
    )
    return h


def driver_header_text(event_c):
    e = ""
    for i in range(1, event_c + 1):
        e += f"Event {i : <5}"
    h = f"\n{'Place' : <10}{'Driver' : <20}{'Car' : <8}" + e + f"{'Points' : <7}"
    return h


def class_header_csv(event_c):
    h = ["Place", "Driver", "Car", "Class"]
    for i in range(1, event_c + 1):
        h.append(f"Event {i}")
    h = h + ["Points", "Cones", "DNF"]
    return h


def driver_header_csv(event_c):
    h = ["Place", "Driver", "Car"]
    for i in range(1, event_c + 1):
        h.append(f"Event {i}")
    h = h + ["Points"]
    return h


def driver_event_points(car_number):
    event_p = []
    for ed in event_dates():
        sql = (
            f"SELECT "
            f"points "
            f"FROM "
            f"driver_results "
            f"JOIN "
            f"drivers on driver_results.driver_id=drivers.id "
            f"WHERE car_number = {car_number} and event_date='{ed}'"
        )
        event_result = execute_read_query(db_conn, sql)
        if len(event_result) == 0:
            event_p.append("0")
        else:
            event_p.append(str(event_result[0][0]))
    return event_p


def class_standings(driver_id, car_class):
    """This will take the drivers id, and class, then pull a list of events.
    It will query event_date and driver/class to get points for that event.
    It will compare the event_date and see if there's an event for that driver and class and
    then append the points to the output, if that event doesn't exist for that driver/class,
    it will append zero points.
    """
    ep = []
    driver_id = driver_id
    car_class = car_class
    sql = f"select distinct(event_date) from class_results"
    results = execute_read_query(db_conn, sql)
    for e in results:
        sql = (
            f"SELECT "
            f"points "
            f"FROM "
            f"class_results "
            f"WHERE "
            f"driver_id={driver_id} and class='{car_class}' and event_date='{e[0]}'"
        )
        results = execute_read_query(db_conn, sql)
        if len(results) == 1:
            ep.append(results[0][0])
        else:
            ep.append(0)
    class_sql = (
        f"SELECT driver_name,"
        f"car_number,"
        f"points,"
        f"cones,"
        f"dnf "
        f"FROM "
        f"class_points "
        f"JOIN "
        f"drivers "
        f"ON "
        f"drivers.id=class_points.driver_id "
        f"WHERE "
        f"class='{car_class}' and driver_id='{driver_id}'"
    )
    results = execute_read_query(db_conn, class_sql)
    cs = [
        results[0][0],
        results[0][1],
        car_class,
        results[0][2],
        results[0][3],
        results[0][4],
    ]
    return cs, ep


def class_point_parser(soup, event_date):
    class_table = soup.find_all("table")[2]
    # = [[cell.text.strip() for cell in row.find_all(["th","td"])]
    #                        for row in class_table.find_all("tr")]i
    car_class = ""
    winner_time = float()
    class_data = table_data(class_table)
    for item in class_data:
        first_element = list_to_string(item[0])
        if len(first_element) == 0:
            continue
        if first_element[0].isalpha():
            car_class = first_element.split(" ")[0]
            if car_class not in non_points:
                print(f"Class: {car_class}")
            continue
        if car_class in non_points:
            continue
        car_number = int(item[2])
        position = first_element.replace("T", "")
        if item[CT] in ["DNS", "DNF"]:
            continue
        final_time = item[CT]
        if position == "1":
            winner_time = float(item[CT])
            print(f"winner_time: {winner_time}")
        driver = item[3].replace("'", "").title()
        points = calc_points(winner_time, float(final_time))
        cones, dnf = get_cone_dnf(item)
        if points_card(car_number):
            print(
                f"Event Date: {event_date} Position: {position} Class: {car_class} Car No: {car_number} "
                f"Driver: {driver} Points: {points} Cones: {cones} DNF: {dnf}"
            )
            # Create driver record if it doesn't exist
            sql = f"SELECT id from drivers where car_number = '{car_number}'"
            driver_results = execute_read_query(db_conn, sql)
            if len(driver_results) == 0:
                sql = f"INSERT into drivers VALUES (NULL,'{driver}',{car_number})"
                driver_id = execute_query(db_conn, sql)
            else:
                driver_id = driver_results[0][0]
            sql = (
                f"INSERT INTO "
                f"class_results "
                f"VALUES "
                f"(NULL,'{event_date}',{driver_id},'{car_class}',{position},{final_time},{points},{cones},{dnf},0,0)"
            )
            execute_query(db_conn, sql)
    return


def driver_point_parser(soup, event_date):
    pax_table = soup.find_all("table")[1]
    # = [[cell.text.strip() for cell in row.find_all(["th","td"])]
    #                        for row in class_table.find_all("tr")]i
    winner_time = float()

    pax_data = table_data(pax_table)
    for item in pax_data:
        first_element = list_to_string(item[0])
        if len(first_element) == 0:
            continue
        if first_element[0].isalpha():
            continue
        car_number = int(item[2])
        if car_number >= 1000:
            continue
        car_class = item[1]
        position = first_element.replace("T", "")
        final_time = item[8]
        if position == "1":
            winner_time = float(item[8])
            print(f"winner_time: {winner_time}")
        driver = item[3].replace("'", "").title()
        if item[6] == "DNF":
            points = 70
        else:
            points = calc_points(winner_time, float(final_time))
        print(
            f"Event Date: {event_date} Pax Position: {position} Car No: {car_number} Driver: {driver} Points: {points}"
        )
        # Create driver record if it doesn't exist
        sql = f"SELECT id from drivers where car_number = '{car_number}'"
        driver_results = execute_read_query(db_conn, sql)
        if len(driver_results) == 0:
            sql = f"INSERT into drivers VALUES (NULL,'{driver}',{car_number})"
            driver_id = execute_query(db_conn, sql)
        else:
            driver_id = driver_results[0][0]
        sql = (
            f"INSERT INTO "
            f"driver_results "
            f"VALUES "
            f"(NULL,'{event_date}',{driver_id},'{car_class}',{position},{final_time},{points},0,0)"
        )
        execute_query(db_conn, sql)
    return


def missed_events(driver_id, car_class):
    """
    This function will insert a zero point result into the class and pax results tables for missed events.
    This row will have a bool `missed` which is needed because previous logic would count a missed event as a zero
    point event.  This could also be fixed by querying against driver_id and event date and treating a Null result.
    """
    ed = event_dates()
    for d in ed:
        sql = (
            f"SELECT "
            f"count(1) "
            f"FROM "
            f"class_results "
            f"WHERE "
            f"driver_id={driver_id} and class='{car_class}' and event_date = '{d}'"
        )
        results = execute_read_query(db_conn, sql)
        if results[0][0] == 0:
            print(
                f"no event found for driver id: {driver_id} class: {car_class} date: {d} creating entry."
            )
            sql = (
                f"INSERT INTO "
                f"class_results "
                f"VALUES "
                f"(NULL,'{d}',{driver_id},'{car_class}',0,0,0,0,0,0,1)"
            )
            execute_query(db_conn, sql)
        # do it for Pax
        sql = (
            f"SELECT "
            f"count(1) "
            f"FROM "
            f"driver_results "
            f"WHERE "
            f"driver_id={driver_id} and event_date = '{d}'"
        )
        results = execute_read_query(db_conn, sql)
        if results[0][0] == 0:
            print(
                f"no pax event found for driver id: {driver_id} date: {d} creating entry."
            )
            sql = (
                f"INSERT INTO "
                f"driver_results "
                f"VALUES "
                f"(NULL, '{d}',{driver_id},NULL,0,0,0,0,1)"
            )
            execute_query(db_conn, sql)
    return


def generate_points():
    """
    zero points table
    get all driver ids
    query class results, pull driver id, car class
    pass driver id and class to total points function
    total points function will calculate drops and return total points for driver/class.
    calculate points for class with drops and store in points table.
    """
    # zero points table
    sql = "delete from class_points"
    execute_query(db_conn, sql)
    sql = "delete from driver_points"
    execute_query(db_conn, sql)
    sql = "Select id from drivers order by id asc"
    driver_id_results = execute_read_query(db_conn, sql)
    for i in driver_id_results:
        driver_id = i[0]
        sql = (
            f"SELECT DISTINCT "
            f"class "
            f"FROM "
            f"class_results "
            f"WHERE "
            f"driver_id={driver_id}"
        )
        driver_class_results = execute_read_query(db_conn, sql)
        for c in driver_class_results:
            car_class = c[0]
            missed_events(driver_id, car_class)
            update_average_points(driver_id, car_class)
            total_points = total_class_points(driver_id, c[0])
            sql = (
                f"SELECT "
                f"sum(cones), "
                f"sum(dnf) "
                f"FROM "
                f"class_results "
                f"WHERE "
                f"driver_id={driver_id} and class='{car_class}' and missed=0"
            )
            result = execute_read_query(db_conn, sql)
            cones, dnf = result[0]
            sql = (
                f"INSERT INTO "
                f"class_points "
                f"VALUES "
                f"(NULL,{driver_id},'{car_class}',{total_points},{cones},{dnf})"
            )
            execute_query(db_conn, sql)
        tdp = total_driver_points(driver_id)
        sql = f"INSERT into driver_points values (NULL,{driver_id},{tdp})"
        execute_query(db_conn, sql)
    return


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "-u",
        "--url",
        help="url to axware html or path local html file",
        action="store",
        dest="url",
        default=None,
        required=False,
    )
    argparser.add_argument(
        "-a",
        "--average",
        help=f"create record for driver that went to a national event.  Requires car number and class name.  "
        f"Use --name for class name.  ie {argparser.prog} -a <CARNUMBER> -n <CLASS> -d MM-DD-YYYY",
        dest="average",
        default=None,
        required=False,
    )
    argparser.add_argument(
        "-n",
        "--name",
        help="class for national",
        dest="car_class",
        default=None,
        required=False,
    )
    argparser.add_argument(
        "-d", "--event_date", help="event_date", default=None, required=False
    )
    argparser.add_argument(
        "-g",
        "--generate",
        help="generate points",
        action="store_true",
        default=None,
        required=False,
    )
    argparser.add_argument(
        "-c",
        "--class_points",
        help="Display class points, can be used with -n/--name",
        action="store_true",
        required=False,
    )
    argparser.add_argument(
        "-o",
        "--output",
        help="Output Format (CSV,Text)",
        default="text",
        required=False,
    )
    argparser.add_argument(
        "-f",
        "--file",
        help="Output Filename",
        action="store",
        dest="file",
        default=None,
        required=False,
    )
    argparser.add_argument(
        "--driver",
        help="Driver Total Points",
        default=False,
        action="store_true",
        required=False,
    )
    argparser.add_argument(
        "--debug",
        help="enable debug",
        default=False,
        action="store_true",
        required=False,
    )
    args = argparser.parse_args()
    global DEBUG
    DEBUG = args.debug
    db_init()

    if args.url:
        if args.url.startswith("http"):
            r = requests.get(args.url)
            link = r.content
        else:
            link = open(args.url)
        soup = BeautifulSoup(link, "html.parser")
        table_count = len(soup.find_all("table"))
        event_date = get_event_date(soup.find_all("table")[0])
        print(f"ed: {event_date}")
        if table_count == 4:
            class_point_parser(soup, event_date)
        elif table_count == 3:
            table = soup.find_all("table")[1]
            if len(table_data(table)) < 5:
                class_point_parser(soup, event_date)
            else:
                driver_point_parser(soup, event_date)
        elif table_count == 2:
            driver_point_parser(soup, event_date)
        else:
            sys.exit("Invalid Input")

    if args.average:
        try:
            event_date = args.event_date
            car_number = args.average
            car_class = args.car_class.upper()
        except Exception as e:
            print(f"Error: {e}")
        # event_date.
        sql = (
            f"SELECT "
            f"class_results.id, "
            f"drivers.id "
            f"FROM class_results "
            f"JOIN "
            f"drivers "
            f"ON "
            f"drivers.id = driver_id "
            f"WHERE "
            f"event_date = '{event_date}' and class = '{car_class}' and car_number = '{car_number}'"
        )
        results = execute_read_query(db_conn, sql)
        if len(results) == 0:
            print("no record found, adding record.")
            # get driver_id
            sql = f"SELECT id from drivers where car_number = '{car_number}'"
            results = execute_read_query(db_conn, sql)
            driver_id = results[0][0]
            sql = (
                f"INSERT INTO"
                f"class_results "
                f"VALUES "
                f"(NULL,'{event_date}',{driver_id},'{car_class}',0,0,0,0,0,1,0)"
            )
            print(sql)
            execute_query(db_conn, sql)
            sql = (
                f"INSERT INTO "
                f"driver_results "
                f"VALUES "
                f"(NULL, '{event_date}',{driver_id},NULL,0,0,0,1,0)"
            )
            execute_query(db_conn, sql)
            generate_points()
            update_average_points(driver_id, car_class)
        else:
            print("Records Found, updating average")
            driver_id = results[0][1]
            sql = (
                f"UPDATE "
                f"class_results "
                f"SET "
                f"national = 1 "
                f"WHERE "
                f"driver_id = {driver_id} and class = '{car_class}' and event_date = '{event_date}'"
            )
            execute_query(db_conn, sql)
            sql = (
                f"UPDATE "
                f"driver_results "
                f"SET "
                f"national = 1 "
                f"WHERE "
                f"driver_id = {driver_id} and event_date = '{event_date}'"
            )
            execute_query(db_conn, sql)
            update_average_points(driver_id, car_class)

    if args.generate:
        generate_points()

    if args.class_points:
        if args.file:
            fh = open(args.file, "w")
        else:
            fh = None
        generate_points()
        car_class = []
        # open filehandle for csv
        event_c = len(event_dates())
        if DEBUG:
            print(f"args.name {args.name} args.car_class: {args.car_class}")
        if args.car_class:
            car_class.append(args.car_class.upper())
        else:
            sql = f"SELECT distinct class from class_points order by class ASC"
            results = execute_read_query(db_conn, sql)
            for cc in results:
                car_class.append(cc[0])
        for c in car_class:
            p = 1
            class_sql = (
                f"SELECT "
                f"driver_id "
                f"FROM "
                f"class_points "
                f"JOIN "
                f"drivers on drivers.id=class_points.driver_id "
                f"WHERE "
                f"class='{c}' order by points DESC"
            )
            results = execute_read_query(db_conn, class_sql)
            if args.output == "text":
                h = class_header_text(event_c)
                print(h, file=fh)
            elif args.output == "csv":
                h = class_header_csv(event_c)
                h_string = ""
                for i in h:
                    h_string += f"{i},"
                print(h_string[:-1], sep="", file=fh)
            for line in results:
                epoints = ""
                row, ep = class_standings(line[0], c)
                if DEBUG:
                    print(row, ep)
                if args.output == "text":
                    for i in ep:
                        epoints += f"{i:<10}"
                    line1 = f"{p : <10}{row[0] : <25}{row[1] : <8}{row[2] : <9}"
                    line2 = f"{row[3] : <10}{row[4] : <8}{row[5] : <8}"
                    print(line1, epoints, line2, sep="", file=fh)
                elif args.output == "csv":
                    for i in ep:
                        epoints += f"{i},"
                    line1 = f"{p},{row[0]},{row[1]},{row[2]},"
                    line2 = f"{row[3]},{row[4]},{row[5]}"
                    print(line1, epoints, line2, sep="", file=fh)
                p += 1
        if fh:
            fh.close()

    if args.driver:
        if args.file:
            fh = open(args.file, "w")
        else:
            fh = None
        generate_points()
        sql = (
            "SELECT "
            "ROW_NUMBER () OVER ( ORDER BY points DESC) RowNum, "
            "driver_name, car_number, points "
            "FROM "
            "driver_points "
            "JOIN "
            "drivers on driver_points.driver_id = drivers.id"
        )
        result = execute_read_query(db_conn, sql)
        event_c = len(event_dates())
        if args.output == "text":
            print(f"{driver_header_text(event_c)}", file=fh)
            for r in result:
                event_points_string = ""
                event_p = driver_event_points(r[2])
                for e in event_p:
                    event_points_string += f"{e : <11}"
                print(
                    f"{r[0] : <10}{r[1] : <20}{r[2] : <8}{event_points_string}{r[3] : <8}",
                    sep="",
                    file=fh,
                )
        if args.output == "csv":
            dh_string = ""
            dhl = driver_header_csv(event_c)
            for i in dhl:
                dh_string += f"{i},"
            print(dh_string[:-1], sep="", file=fh)
            for r in result:
                event_points_string = ""
                event_p = driver_event_points(r[2])
                for e in event_p:
                    event_points_string += f"{e},"
                print(
                    f"{r[0]},{r[1]},{r[2]},{event_points_string}{r[3]}",
                    sep="",
                    file=fh,
                )
        if fh:
            fh.close()


if __name__ == "__main__":
    main()
