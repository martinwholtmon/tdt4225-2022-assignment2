import os
from DbConnector import DbConnector
from tabulate import tabulate


class InsertData:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, tables: list):
        """Create tables and insert into DB
        Tables = [
            (
                "CREATE TABLE `employees` ("
                "  `emp_no` int(11) NOT NULL AUTO_INCREMENT,"
                "  `birth_date` date NOT NULL,"
                "  PRIMARY KEY (`emp_no`)"
                ")"
            ),
            (
                "CREATE TABLE `departments` ("
                "  `dept_no` char(4) NOT NULL,"
                "  `dept_name` varchar(40) NOT NULL,"
                "  PRIMARY KEY (`dept_no`)"
                ")"
            )
        ]

        Source: https://dev.mysql.com/doc/connector-python/en/connector-python-example-ddl.html
        """
        for table in tables:
            self.cursor.execute(table)
        self.db_connection.commit()

    def insert_data(self, table_name, columns, value):
        # prepare data
        columns = ", ".join(columns)
        # Basically, convert the values and add single quote around strings
        value = ", ".join(
            map(lambda x: "'" + str(x) + "'" if isinstance(x, str) else str(x), value)
        )

        # Prepare query
        query = "INSERT INTO %s(%s) VALUES(%s)"

        # Insert
        self.cursor.execute(query % (table_name, columns, value))
        self.db_connection.commit()
        return self.cursor.lastrowid

    def insert_batch_data(self, table_name, columns, values):
        # prepare data
        columns = ", ".join(columns)
        values = [
            ", ".join(
                map(lambda x: "'" + str(x) + "'" if isinstance(x, str) else str(x), i)
            )
            for i in values
        ]

        # Prepare query
        query = "INSERT INTO %s(%s)" % (table_name, columns)
        query = query + " VALUES(%s)"

        # Insert
        self.cursor.executemany(query, values)
        self.db_connection.commit()

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


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


def parse_and_insert_dataset(program: InsertData):
    # program.insert_data("test", ["v1", "v2"], [["1", "2"], ["3", "4"]])

    # Insert user
    # If activity (has label) -> insert activity
    #   Get id from db -> batch insert TrackPoint for that activity
    # else -> batch insert trackpoints

    labeled_ids = read_labeled_users_file(r".\dataset\labeled_ids.txt")
    user = ""
    labels = []
    has_labels = False
    for root, dirs, files in os.walk(".\dataset\data"):
        # New user?
        if len(dirs) > 0 and dirs[0] == "Trajectory":
            user = os.path.normpath(root).split(os.path.sep)[-1]
            print(user)

            # Get labels
            if user in labeled_ids and files[0] == "labels.txt":
                labels = read_user_labels_file(os.path.join(root, files[0]))
                has_labels = True
            else:
                has_labels = False

            # insert user into db
            _ = program.insert_data("User", ["id", "has_labels"], [user, has_labels])

            # insert activity, store ids in the dict
            if has_labels:
                insert_activity(user, labels, program)

    # Insert Trajectory


def insert_activity(user, labels, program):
    for key, val in labels.items():
        data = val["data"]

        start_date_time = str(data[0]).replace("/", "-") + " " + str(data[1])
        end_date_time = str(data[2]).replace("/", "-") + " " + str(data[3])

        insertion_id = program.insert_data(
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


def read_labeled_users_file(path) -> list:
    """Will read the labeled_ids.txt that includes all the users that have labels

    Args:
        path (str): path to file

    Returns:
        list: list of ids
    """
    file = open(path, "r")
    list_of_lists = [(line.strip()) for line in file]
    file.close()
    return list_of_lists


def read_user_labels_file(path) -> dict:
    data = read_data_file(path)[1:]  # skip header
    labels = {}
    for d in data:
        key = str(d[0]).replace("/", "") + str(d[1]).replace(":", "")
        labels[key] = {}
        labels[key]["data"] = d
    return labels


def read_data_file(path) -> "list[list]":
    file = open(path, "r")
    list_of_lists = [(line.strip()).split() for line in file]
    file.close()
    return list_of_lists


def main():
    program = None
    tables = create_tables()
    try:
        program = InsertData()

        # Drop tables
        program.drop_table(table_name="TrackPoint")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="User")

        program.create_table(tables)
        parse_and_insert_dataset(program)

        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()