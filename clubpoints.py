#!/usr/bin/env python3

from configparser import ConfigParser
import requests
import importlib
from bs4 import BeautifulSoup
import argparse
from dateutil import parser
import json
import sys
import os
import sqlite3
import re
from sqlite3 import Error
import csv


config = ConfigParser()
config.read("config.ini")
# get the club name and use it to pull in modules
club = config.get("region", "club")
CT = int(config.get("region","CT"))
# importlib will let you use import_module but it is imported into it's own namespace so we move the fuctions to global NS.
ns = importlib.import_module(club)
calc_points = ns.calc_points

points_card = ns.points_card
calc_drops = ns.calc_drops

database_name = f"{club}_points.db"
non_points = ["TO", "X"]
global DEBUG
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
    unique (event_date,driver_id)
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
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


def event_count():
    # find number of events
    sql = f"select count(distinct event_date) from class_results"
    results = execute_read_query(db_conn, sql)
    event_count = results[0][0]
    return event_count


def event_dates():
    event_d = []
    sql = f"select distinct event_date from class_results"
    results = execute_read_query(db_conn, sql)
    for d in results:
        event_d.append(d[0])
    return event_d


def update_average_points(driver_id, car_class):
    sum_points = float()
    sql = f"SELECT points from class_results where class = '{car_class}' and driver_id = '{driver_id}' and national = 0"
    points = execute_read_query(db_conn, sql)
    points_count = len(points)
    print(driver_id)
    for n in points:
        sum_points += n[0]
    avg_points = sum_points / points_count
    avg_points = round(avg_points, 3)
    sql = f"UPDATE class_results set points = {avg_points} where  class = '{car_class}' and driver_id = '{driver_id}' and national = 1"
    execute_query(db_conn, sql)
    sum_driver_points = float()
    sql = f"SELECT points from driver_results where driver_id = '{driver_id}' and national = 0"
    driver_points = execute_read_query(db_conn, sql)
    points_count = len(driver_points)
    for n in driver_points:
        sum_driver_points += n[0]
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
    if DEBUG:
        print(f"number of events: {len(class_points_results)} drops: {drops}")
    count = len(driver_points_results) - drops
    for p in driver_points_results:
        rp.append(p[0])
    rp.sort(reverse=True)
    rp = rp[:count]
    dp = round(sum(rp), 3)
    if DEBUG:
        print(f"driver points: {dp}")
    return dp


def listToString(s):
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
    for l in header_data:
        result = re.search(date_mask, str(l))
        if result is not None:
            return result.group()


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
    event_c = event_c
    e = ""
    for i in range(1, event_c + 1):
        e += f"{'Event '}{i : <5}"
    h = (
        f"\n{'Place' : <10}{'Driver' : <25}{'Car' : <8}{'Class' : <10}"
        + e
        + f"{' Points' : <11}{'Cones' : <8}{'DNF' : <5}"
    )
    return h


def driver_header_text(event_c):
    event_c = event_c
    e = ""
    for i in range(1, event_c + 1):
        e += f"Event {i : <5}"
    h = f"\n{'Place' : <10}{'Driver' : <20}{'Car' : <8}" + e + f"{'Points' : <7}"
    return h


def class_header_csv(event_c):
    event_c = event_c
    h = ["Place", "Driver", "Car", "Class"]
    for i in range(1, event_c + 1):
        h.append(f"Event {i}")
    h = h + ["Points", "Cones", "DNF"]
    return h


def driver_header_csv(event_c):
    event_c = event_c
    h = ["Place", "Driver", "Car"]
    for i in range(1, event_c + 1):
        h.append(f"Event {i}")
    h = h + ["Points"]
    return h


def class_standings(driver_id, car_class):
    """This will take the drivers id, and class, then pull a list of events.  It will query event_date and driver/class to get points for that
    event.  It will compare the event_date and see if there's an event for that driver and class then append the points to the output, if that
    event doesn't exist for that driver/class it will append zero points."""
    cs = []
    ep = []
    driver_id = driver_id
    car_class = car_class
    sql = f"select distinct(event_date) from class_results"
    results = execute_read_query(db_conn, sql)
    for e in results:
        sql = f"select points from class_results where driver_id={driver_id} and class='{car_class}' and event_date='{e[0]}'"
        results = execute_read_query(db_conn, sql)
        if len(results) == 1:
            ep.append(results[0][0])
        else:
            ep.append(0)
    class_sql = f"SELECT driver_name,car_number,points,cones,dnf from class_points join drivers on drivers.id=class_points.driver_id where class='{car_class}' and driver_id='{driver_id}'"
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

    class_data = table_data(class_table)
    for item in class_data:
        row_length = len(item)
        first_element = listToString(item[0])
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
        driver = item[3].replace("'", "")
        points = calc_points(winner_time, float(final_time))
        cones, dnf = get_cone_dnf(item)
        if points_card(car_number):
          print(
              f"Event Date: {event_date} Position: {position} Class: {car_class} Car No: {car_number} Driver: {driver} Points: {points} Cones: {cones} DNF: {dnf}"
          )
          # Create driver record if it doesn't exist
          sql = f"SELECT id from drivers where car_number = '{car_number}'"
          driver_results = execute_read_query(db_conn, sql)
          if len(driver_results) == 0:
            sql = f"INSERT into drivers VALUES (NULL,'{driver}',{car_number})"
            driver_id = execute_query(db_conn, sql)
          else:
            driver_id = driver_results[0][0]
          sql = f"INSERT INTO class_results VALUES (NULL,'{event_date}',{driver_id},'{car_class}',{position},{final_time},{points},{cones},{dnf},0)"
          execute_query(db_conn, sql)
    return


def driver_point_parser(soup, event_date):
    pax_table = soup.find_all("table")[1]
    # = [[cell.text.strip() for cell in row.find_all(["th","td"])]
    #                        for row in class_table.find_all("tr")]i

    pax_data = table_data(pax_table)
    for item in pax_data:
        row_length = len(item)
        first_element = listToString(item[0])
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
        driver = item[3].replace("'", "")
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
        sql = f"INSERT INTO driver_results VALUES (NULL,'{event_date}',{driver_id},'{car_class}',{position},{final_time},{points},0)"
        execute_query(db_conn, sql)
    return


def generate_points():
    """
    zero points table
    get all driver ids
    query class results, pull driver id, car class
    pass driver id and class to total points function which will calculate drops and return total points for driver/class.
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
        sql = f"select distinct class from class_results where driver_id={driver_id}"
        driver_class_results = execute_read_query(db_conn, sql)
        for c in driver_class_results:
            car_class = c[0]
            update_average_points(driver_id, car_class)
            total_points = total_class_points(driver_id, c[0])
            sql = f"SELECT sum(cones), sum(dnf) from class_results where driver_id={driver_id} and class='{car_class}'"
            result = execute_read_query(db_conn, sql)
            cones, dnf = result[0]
            sql = f"INSERT into class_points values (NULL,{driver_id},'{car_class}',{total_points},{cones},{dnf})"
            result = execute_query(db_conn, sql)
        tdp = total_driver_points(driver_id)
        sql = f"INSERT into driver_points values (NULL,{driver_id},{tdp})"
        result = execute_query(db_conn, sql)
    return


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "-u",
        "--url",
        help="url to axware html",
        action="store",
        dest="url",
        default=None,
        required=False,
    )
    argparser.add_argument(
        "-n",
        "--national",
        help="car number for national",
        dest="national",
        default=None,
        required=False,
    )
    argparser.add_argument(
        "-c", "--car_class", help="class for national", default=None, required=False
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
        "-p",
        "--print_points",
        help="Display points, can be used with -c/--car_class",
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
        "-f", "--filename", help="CSV Filename", default="results.csv", required=False
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
    DEBUG = args.debug
    db_init()

    if args.url:
        if args.url.startswith('http'):
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
          class_table = soup.find_all("table")[2]
          table = soup.find_all("table")[1]
          if len(table_data(table)) < 5:
            class_point_parser(soup, event_date)
          else:
            driver_point_parser(soup, event_date)
        elif table_count == 2:
            driver_point_parser(soup, event_date)
        else:
            sys.exit("Invalid Input")

    if args.national:
        event_date = args.event_date
        car_number = args.national
        car_class = args.car_class.upper()
        # event_date.
        sql = f"SELECT class_results.id,drivers.id from class_results JOIN drivers on drivers.id = driver_id where event_date = '{event_date}' and class = '{car_class}' and car_number = '{car_number}'"
        results = execute_read_query(db_conn, sql)
        if len(results) == 0:
            print("no record found, adding record.")
            # get driver_id
            sql = f"SELECT id from drivers where car_number = '{car_number}'"
            results = execute_read_query(db_conn, sql)
            driver_id = results[0][0]
            sql = f"INSERT into class_results VALUES (NULL,'{event_date}',{driver_id},'{car_class}',0,0,0,0,0,1)"
            print(sql)
            results = execute_query(db_conn, sql)
            sql = f"INSERT into driver_results VALUES (NULL, '{event_date}',{driver_id},NULL,0,0,0,1)"
            results = execute_query(db_conn, sql)
            generate_points()
            update_average_points(driver_id, car_class)
        else:
            print("Records Found, updating average")
            driver_id = results[0][1]
            update_average_points(driver_id, car_class)

    if args.generate:
        generate_points()

    if args.print_points:
        generate_points()
        car_class = []
        # open filehandle for csv
        if args.output == "csv":
            fn = args.filename
            fh = open(fn, "w")
            writer = csv.writer(fh, delimiter=",", quotechar='"')
        event_c = event_count()
        if args.car_class:
            car_class.append(args.car_class.upper())
        else:
            sql = f"SELECT distinct class from class_points order by class ASC"
            results = execute_read_query(db_conn, sql)
            for cc in results:
                car_class.append(cc[0])
        for c in car_class:
            p = 1
            class_sql = f"SELECT driver_id from class_points join drivers on drivers.id=class_points.driver_id where class='{c}' order by points DESC"
            results = execute_read_query(db_conn, class_sql)
            epoints = ""
            if args.output == "text":
                h = class_header_text(event_c)
                print(h)
            else:
                writer.writerow(class_header_csv(event_c))
            for l in results:
                row, ep = class_standings(l[0], c)
                if DEBUG:
                    print(row, ep)
                if args.output == "text":
                    for i in ep:
                        epoints += f"{i:<11}"
                    line1 = f"{p : <10}{row[0] : <25}{row[1] : <8}{row[2] : <9}"
                    line2 = f"{row[3] : <10}{row[4] : <8}{row[5] : <8}"
                    print(line1, epoints, line2)
                elif args.output == "csv":
                    r = [p, row[0], row[1], row[2]]
                    for i in ep:
                        r.append(i)
                    r = r + [row[3], row[4], row[5]]
                    writer.writerow(r)
                epoints = ""
                p += 1
        if args.output == "csv":
            fh.close()

    if args.driver:
        generate_points()
        sql = "select ROW_NUMBER () OVER ( ORDER BY points DESC) RowNum, driver_name, car_number, points from driver_points join drivers on driver_points.driver_id = drivers.id"
        result = execute_read_query(db_conn, sql)
        event_c = event_count()
        if args.output == "text":
            print(f"{driver_header_text(event_c)}")
            for r in result:
                event_p = []
                event_points_string = ""
                for ed in event_dates():
                    sql = f"SELECT points from driver_results join drivers on driver_results.driver_id=drivers.id where car_number = {r[2]} and event_date='{ed}'"
                    event_result = execute_read_query(db_conn, sql)
                    if len(event_result) == 0:
                        event_p.append("0")
                    else:
                        event_p.append(str(event_result[0][0]))
                for e in event_p:
                    event_points_string += f"{e : <11}"
                print(
                    f"{r[0] : <10}{r[1] : <20}{r[2] : <8}{event_points_string}{r[3] : <8}"
                )
        if args.output == "csv":
            dh_string = ""
            dhl = driver_header_csv(event_c)
            for i in dhl:
                dh_string += f"{i},"
            print(dh_string[:-1])
            for r in result:
                event_p = []
                event_points_string = ""
                for ed in event_dates():
                    sql = f"SELECT points from driver_results join drivers on driver_results.driver_id=drivers.id where car_number = {r[2]} and event_date='{ed}'"
                    event_result = execute_read_query(db_conn, sql)
                    if len(event_result) == 0:
                        event_p.append("0")
                    else:
                        event_p.append(str(event_result[0][0]))
                for e in event_p:
                    event_points_string += f"{e},"
                print(f"{r[0]},{r[1]},{r[2]},{event_points_string}{r[3]}")


if __name__ == "__main__":
    main()
