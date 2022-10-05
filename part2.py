import itertools
from haversine import haversine, Unit
from tomlkit import datetime
from DbHandler import DbHandler


def task_1(db):
    print("\nTask 1")
    print(f"User: {db.get_nr_rows('User')}")
    print(f"Activity: {db.get_nr_rows('Activity')}")
    print(f"TrackPoint: {db.get_nr_rows('TrackPoint')}")


def task_2(db):
    print("\nTask 2")
    query = """
    SELECT AVG(nr_activities) 
        FROM (
          SELECT user_id, count(*) as nr_activities
          FROM test_db.Activity
          GROUP BY user_id
        ) as activities;
    """
    ret = db.execute_query(query)
    print(f"Average number of activities per user is {ret[0][0]}")


def task_3(db):
    print("\nTask 3")
    query = """
        SELECT 
          user_id, count(*) as nr_activities 
        FROM test_db.Activity 
        GROUP BY user_id 
        ORDER BY nr_activities DESC 
        LIMIT 20;
    """
    ret = db.execute_query(query)
    print(f"Users with the most activity: {ret}")


def task_4(db):
    print("\nTask 4")
    query = """
        SELECT user_id 
        FROM test_db.Activity 
        WHERE transportation_mode = 'taxi' 
        GROUP BY user_id;
    """
    ret = db.execute_query(query)
    print(f"Users that have taken a taxi: {ret}")


def task_5(db):
    print("\nTask 5")
    query = """
        SELECT 
            transportation_mode, count(*) as nr_activities 
        FROM test_db.Activity 
        WHERE transportation_mode IS NOT NULL 
        GROUP BY transportation_mode;
    """
    ret = db.execute_query(query)
    print(f"Transportation mode with count: {ret}")


def task_6(db):
    print("\nTask 6")
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
    year = ret[0]
    print(f"Year with most activities: {ret}")

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
    most_recorded_hours = max(recorded_hours, key=recorded_hours.get)
    print(f"Year most most recorded hours: {most_recorded_hours}")


def task_7(db):
    print("\nTask 7")
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
    print(f"User 112 walked {distance} km in 2008")


def task_8(db):
    print("\nTask 8")
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
    print(f"The 20 users who gained the most altitude meters is: {top_users}")


def task_9(db):
    print("\nTask 9")
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
    print(f"Users with invalid activities: {users}")


def task_10(db):
    print("\nTask 10")
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
    print(f"Users that have visited 'the Forbidden City': {ret}")


def task_11(db):
    print("\nTask 11")
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
    print(f"Users with most used transportation mode: {users}")


def main():
    db = None
    try:
        db = DbHandler()
        db.show_tables()

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
