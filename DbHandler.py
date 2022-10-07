from DbConnector import DbConnector
from tabulate import tabulate


class DbHandler:
    """The Database handler. Containing all functionality to interact with the database"""

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
            ),
            ...
        ]

        Source: https://dev.mysql.com/doc/connector-python/en/connector-python-example-ddl.html
        """
        for table in tables:
            self.cursor.execute(table)
        self.db_connection.commit()

    def insert_user(self, values):
        """Insert a user into the DB.

        Args:
            values (list | tuple): The values for a user, e.g.: ["000", False]
        """
        query = "INSERT INTO User (id, has_labels) values (%s, %s)"
        print(f"\nInserting user {values[0]}")

        # Insert
        self.cursor.execute(query, values)
        self.db_connection.commit()

    def insert_activity(self, values) -> "int | None":
        """Insert an activity

        Args:
            values (list | tuple): The values for an activity.

        Returns:
            int | None: The value generated by the DB for the activity
        """
        query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) values (%s, %s, %s, %s)"
        # print(f"Inserting Activity {values[2]} for {values[0]}")

        # Insert
        self.cursor.execute(query, values)
        self.db_connection.commit()
        return self.cursor.lastrowid

    def insert_trackpoints(self, values, partition=100):
        """Insert multiple trackpoints trackpoint

        Args:
            values (list[list | tuple]): A list of trackpoints
        """
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) values "
        print(f"  inserting {len(values)} trackpoints")

        # Insert
        # Because executemany is slow, this will prepare a query with n trackpoints
        prepared_values = ""
        for i, value in enumerate(values):
            prepared_values += (
                "(" + ", ".join(map(lambda x: "'" + str(x) + "'", value)) + ")"
            )

            if (i % partition == 0 and i != 0) or i == len(values) - 1:
                prepared_values += ";"
                self.cursor.execute(query + prepared_values)
                prepared_values = ""
            else:
                prepared_values += ","
        self.db_connection.commit()

    def drop_table(self, table_name: str):
        """Drop a table from the database

        Args:
            table_name (str): name of table
        """
        print(f"Dropping table {table_name}...")
        query = "DROP TABLE IF EXISTS %s"
        self.cursor.execute(query % table_name)  # TODO: Avoid direct substitution

    def show_tables(self):
        """Print tables in database"""
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def execute_query(self, query):
        # execute
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_nr_rows(self, table) -> int:
        """Get number of rows from table"""
        query = "SELECT count(*) as count FROM %s"

        # execute
        self.cursor.execute(query % table)  # TODO: Avoid direct substitution
        return int(self.cursor.fetchall()[0][0])
