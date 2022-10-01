from DbConnector import DbConnector
from tabulate import tabulate


class DbHandler:
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

    def insert_user(self, values):
        query = "INSERT INTO User(id, has_labels) VALUES(%s, %s)"

        # Insert
        self.cursor.execute(query, values)
        self.db_connection.commit()
        return self.cursor.lastrowid

    def insert_activity(self, values):
        query = "INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES(%s, %s, %s, %s)"

        # Insert
        self.cursor.execute(query, values)
        self.db_connection.commit()
        return self.cursor.lastrowid

    def insert_trackpoints(self, values):
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        print(len(values))

        # Insert
        self.cursor.executemany(query, values)
        self.db_connection.commit()

    def drop_table(self, table_name):
        """Drop a table

        Args:
            table_name (str): name of table
        """
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        """Print tables in database"""
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def prepare_values(value: list) -> str:
    return ", ".join(
        map(lambda x: "'" + str(x) + "'" if isinstance(x, str) else str(x), value)
    )
