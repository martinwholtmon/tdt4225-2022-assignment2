from email import header
import itertools
import pandas as pd
from haversine import haversine, Unit
from DbHandler import DbHandler
from tabulate import tabulate


def task_1(db):
    tables = {}
    tables["User"] = db.get_nr_rows("User")
    tables["Activity"] = db.get_nr_rows("Activity")
    tables["TrackPoint"] = db.get_nr_rows("TrackPoint")

    # Print
    print("\nTask 1")
    print(
        f"The 20 users who gained the most altitude meters is: \n{tabulate_dict(tables, ['Table', 'Count'])}"
    )


def task_2(db):
    query = """
    SELECT AVG(nr_activities) 
        FROM (
          SELECT user_id, count(*) as nr_activities
          FROM test_db.Activity
          GROUP BY user_id
        ) as activities;
    """
    ret = db.execute_query(query)

    # Print
    print("\nTask 2")
    print(f"Average number of activities per user is {ret[0][0]}")


def task_3(db):
    query = """
        SELECT 
          user_id, count(*) as nr_activities 
        FROM test_db.Activity 
        GROUP BY user_id 
        ORDER BY nr_activities DESC 
        LIMIT 20;
    """
    ret = db.execute_query(query)

    # Print
    print("\nTask 3")
    print(f"Users with the most activity: \n{tabulate(ret, headers=['User', 'Count'])}")


def task_4(db):
    query = """
        SELECT user_id 
        FROM test_db.Activity 
        WHERE transportation_mode = 'taxi' 
        GROUP BY user_id;
    """
    ret = db.execute_query(query)

    # Print
    print("\nTask 4")
    print(f"Users that have taken a taxi: \n{tabulate(ret ,headers=['User'])}")


def task_5(db):
    query = """
        SELECT 
            transportation_mode, count(*) as nr_activities 
        FROM test_db.Activity 
        WHERE transportation_mode IS NOT NULL 
        GROUP BY transportation_mode;
    """
    ret = db.execute_query(query)

    # Print
    print("\nTask 5")
    print(
        f"Transportation mode with count: \n{tabulate(ret, headers=['Transportation Mode', 'Count'])}"
    )


def task_6(db):
    # Get year with most activities
    query = """
        SELECT 
            YEAR(start_date_time) as year, count(*) as nr_activities 
        FROM test_db.Activity 
        GROUP BY year 
        ORDER BY nr_activities DESC 
        LIMIT 1;
    """
    ret = db.execute_query(query)[0]
    most_activities_year = ret[0]

    # Get year with most recorded hours
    query = "SELECT YEAR(start_date_time), start_date_time, end_date_time FROM test_db.Activity;"
    ret = db.execute_query(query)
    recorded_hours = {}
    for year, start, finish in ret:
        recorded_hours[year] = (
            recorded_hours[year] + (finish - start)
            if recorded_hours.get(year) is not None
            else (finish - start)
        )

    # Convert to hours
    # source: https://stackoverflow.com/a/47207182
    for key, val in recorded_hours.items():
        recorded_hours[key] = divmod(val.seconds, 3600)[0]

    # Most hours
    most_recorded_hours_year = max(recorded_hours, key=recorded_hours.get)

    # Print
    print("\nTask 6")
    print(f"Year with most activities: {most_activities_year}")
    print(f"Year most most recorded hours: {most_recorded_hours_year}")


def task_7(db):
    # Get total distance for user 112 in 2008 with activity = walk
    query = """
        SELECT activity_id, lat, lon 
        FROM test_db.TrackPoint 
        WHERE activity_id IN ( 
            SELECT id 
            FROM test_db.Activity 
        WHERE user_id = '112' AND transportation_mode = 'walk' 
        );
    """
    ret = db.execute_query(query)

    # Calulates the distance between the points
    # Assumes that the points are in order for each activity
    distance = 0.0
    activity_id = ret[0][0]
    for x in range(1, len(ret)):
        if activity_id == ret[x][0]:
            distance += haversine(ret[x - 1][1:], ret[x][1:], unit=Unit.KILOMETERS)
        else:
            # New activity? Update id
            activity_id = ret[x][0]

    # Print
    print("\nTask 7")
    print(f"User 112 walked {round(distance, 3)} km in 2008")


def task_8(db):
    query = """
        SELECT 
            Activity.user_id, TrackPoint.activity_id, TrackPoint.altitude 
        FROM test_db.TrackPoint 
        INNER JOIN Activity ON TrackPoint.activity_id=Activity.id;
    """
    ret = db.execute_query(query)

    altitude = {}
    current_aid = -1
    old_alt = -1
    for uid, aid, alt in ret:
        # Same activity and new alt is higher
        if aid == current_aid and old_alt < alt:
            diff = alt - old_alt
            # add altitude
            altitude[uid] = (
                altitude[uid] + diff if altitude.get(uid) is not None else diff
            )
        else:
            # New activity
            current_aid = aid
        old_alt = alt

    # Sort dict
    # source: https://stackoverflow.com/a/2258273
    altitude = dict(sorted(altitude.items(), key=lambda x: x[1], reverse=True))
    top_users = dict(itertools.islice(altitude.items(), 20))

    # Print
    print("\nTask 8")
    print(
        f"The 20 users who gained the most altitude meters is: \n{tabulate_dict(top_users, ['User', 'Gained Altitude'])}"
    )


def task_9(db):
    query = """
        SELECT 
            Activity.user_id, TrackPoint.activity_id, TrackPoint.date_time 
        FROM test_db.TrackPoint 
        INNER JOIN Activity ON TrackPoint.activity_id=Activity.id;
    """
    ret = db.execute_query(query)

    users = {}
    curr_aid = -1
    old_dt = None
    for uid, aid, dt in ret:
        if aid == curr_aid:
            diff = divmod((dt - old_dt).total_seconds(), 60)[0]
            if diff >= 5:
                users[uid] = users[uid] + 1 if users.get(uid) is not None else 1
        else:
            curr_aid = aid
        old_dt = dt

    # Print
    print("\nTask 9")
    print(
        f"Users with invalid activities: \n{tabulate_dict(users, ['User', 'Invalid Activities'])}"
    )


def task_10(db):
    query = """
        SELECT Activity.user_id
        FROM (
            SELECT 
                activity_id, ROUND(lat, 3) AS lat, ROUND(lon, 3) AS lon
            FROM test_db.TrackPoint
            HAVING lat = '39.916' AND lon = '116.397'
        ) as activities
        INNER JOIN Activity ON activities.activity_id=Activity.id
        GROUP BY user_id;
    """
    ret = db.execute_query(query)

    # Print
    print("\nTask 10")
    print(
        f"Users that have visited 'the Forbidden City': \n{tabulate(ret, headers=['User'])}"
    )


def task_11(db):
    query = """
        SELECT 
            user_id, transportation_mode, COUNT(*) as count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode, user_id
        ORDER BY user_id ASC, count DESC
    """
    ret = db.execute_query(query)
    users = {}
    for uid, mode, _ in ret:
        if users.get(uid) is None:
            users[uid] = mode

    # Print
    print("\nTask 11")
    print(
        f"Users with most used transportation mode: \n{tabulate_dict(users, ['User', 'Transportation Mode'])}"
    )


def tabulate_dict(dict, headers) -> str:
    """Will tabulate a dict that has the format of key:value

    Args:
        dict (dict): a key:value pair store
        headers (list): list of the header names

    Returns:
        str: _description_
    """
    df = pd.DataFrame(dict, index=[0]).transpose()
    return tabulate(df, headers=headers, floatfmt=".0f")


def main():
    db = None
    try:
        db = DbHandler()
        db.show_tables()

        # Tasks:
        task_1(db)
        task_2(db)
        task_3(db)
        task_4(db)
        task_5(db)
        task_6(db)
        task_7(db)
        task_8(db)
        task_9(db)
        task_10(db)
        task_11(db)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == "__main__":
    main()
