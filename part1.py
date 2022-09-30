import os
from DbHandler import DbHandler
from FileHandler import read_data_file, read_labeled_users_file, read_user_labels_file


def create_tables() -> list:
    """Create the tables for the database

    Returns:
        list: a list of all the tables to insert
    """
    tables = []

    # User
    tables.append(
        (
            "CREATE TABLE IF NOT EXISTS `User` ("
            "  `id` varchar(3) NOT NULL,"
            "  `has_labels` boolean NOT NULL,"
            "  PRIMARY KEY (`id`)"
            ")"
        )
    )

    # Activity
    tables.append(
        (
            "CREATE TABLE IF NOT EXISTS `Activity` ("
            "  `id` INT NOT NULL AUTO_INCREMENT,"
            "  `user_id` varchar(3) NOT NULL,"
            "  `transportation_mode` varchar(40),"
            "  `start_date_time` DATETIME,"
            "  `end_date_time` DATETIME,"
            "  PRIMARY KEY (`id`),"
            "  FOREIGN KEY (`user_id`) REFERENCES User(id)"
            ")"
        )
    )

    # TrackPoint
    tables.append(
        (
            "CREATE TABLE IF NOT EXISTS `TrackPoint` ("
            "  `id` INT NOT NULL AUTO_INCREMENT,"
            "  `activity_id` INT,"
            "  `lat` DOUBLE,"
            "  `lon` DOUBLE,"
            "  `altitude` INT,"
            "  `date_days` DOUBLE,"
            "  `date_time` DATETIME,"
            "  PRIMARY KEY (`id`),"
            "  FOREIGN KEY (`activity_id`) REFERENCES Activity(id)"
            ")"
        )
    )
    return tables


def parse_and_insert_dataset(db: DbHandler, stop_at_user=""):
    """Parse the dataset and insert it into the database

    Args:
        program (DbHandler): the database
    """
    # program.insert_data("test", ["v1", "v2"], [["1", "2"], ["3", "4"]])

    # Insert user
    # If activity (has label) -> insert activity
    #   Get id from db -> batch insert TrackPoint for that activity
    # else -> batch insert trackpoints
    path_to_dataset = os.path.join("dataset")

    labeled_ids = read_labeled_users_file(
        os.path.join(path_to_dataset, "labeled_ids.txt")
    )
    user = ""
    labels = []
    has_labels = False
    for root, dirs, files in os.walk(os.path.join(path_to_dataset, "Data")):
        # New user?
        if len(dirs) > 0 and dirs[0] == "Trajectory":
            user = os.path.normpath(root).split(os.path.sep)[-1]
            print(user)

            # Partial insert, 0..stop_at_user-1
            if user == stop_at_user:
                return

            # Get labels
            if user in labeled_ids and files[0] == "labels.txt":
                labels = read_user_labels_file(os.path.join(root, files[0]))
                has_labels = True
            else:
                has_labels = False

            # insert user into db
            _ = db.insert_data("User", ["id", "has_labels"], [user, has_labels])

            # insert activity, store ids in the dict
            if has_labels:
                insert_activity(user, labels, db)

        # Insert Trajectory
        if os.path.normpath(root).split(os.path.sep)[-1] == "Trajectory":
            values = []
            for file in files:
                insert_trajectory(root, file, has_labels, labels, values)
            db.insert_trackpoints(
                values,
            )


def insert_activity(user, labels, db: DbHandler):
    """Will insert an activity into the db for a user.
    Will update the dict with the id of the activity to use later when inserting TrackPoint

    Args:
        user (str): id of the user
        labels (dict): All the activities/labels
        program (DbHandler): the database controller
    """
    for key, val in labels.items():
        data = val["data"]

        start_date_time = get_datetime_format(data[0], data[1])
        end_date_time = get_datetime_format(data[2], data[3])

        insertion_id = db.insert_data(
            "Activity",
            [
                "user_id",
                "transportation_mode",
                "start_date_time",
                "end_date_time",
            ],
            [user, str(data[4]), start_date_time, end_date_time],
        )

        # update dict with id
        labels[key]["id"] = insertion_id


def insert_trajectory(root, file, has_labels, labels, values):
    path = os.path.join(root, file)
    data_points = read_data_file(path)[6:]
    if len(data_points) > 2500:
        return

    # Prepare data for insertion
    for data in data_points:
        lat = data[0]
        lon = data[1]
        altitude = int(data[3])
        date_days = data[4]
        date_time = get_datetime_format(data[5], data[6])
        # if has_labels:
        #     activity_id = "000"
        # else:
        #     activity_id = "000"

        # Append to values
        values.append([lat, lon, altitude, date_days, date_time])


def get_datetime_format(date, time) -> str:
    return str(date).replace("/", "-") + " " + str(time)


def main():
    db = None
    tables = create_tables()
    try:
        db = DbHandler()

        # Drop tables
        db.drop_table(table_name="TrackPoint")
        db.drop_table(table_name="Activity")
        db.drop_table(table_name="User")

        db.create_table(tables)
        parse_and_insert_dataset(db, "011")

        db.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == "__main__":
    main()
