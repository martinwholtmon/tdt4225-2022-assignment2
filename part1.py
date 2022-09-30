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
            "  `lat` DOUBLE NOT NULL,"
            "  `lon` DOUBLE NOT NULL,"
            "  `altitude` INT NOT NULL,"
            "  `date_days` DOUBLE NOT NULL,"
            "  `date_time` DOUBLE NOT NULL,"
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

        start_date_time = str(data[0]).replace("/", "-") + " " + str(data[1])
        end_date_time = str(data[2]).replace("/", "-") + " " + str(data[3])

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
