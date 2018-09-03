import re
import MySQLdb
from pprint import pprint


class Table:

    columns = []
    values = []
    primary = False

    def __init__(self, table, source, destination, create, replace):
        self.table = table
        self.source = source
        self.destination = destination
        self.create = create
        self.replace = replace
        self.insert_stmt = self.get_insert_base()

    def build_table(self):
        self.check_table()
        self.create_table()

    def copy_data(self):
        data_cursor = self.source.connection.cursor()
        data_insert = self.destination.connection.cursor()
        query = self.build_query()

        data_cursor.execute(query)

        try:
            for record in data_cursor:
                data = self.get_insert_record(record)
                data_insert.execute(self.insert_stmt, data)

            self.destination.connection.commit()
        except MySQLdb.Error as error:
            pprint(error)
            self.destination.connection.rollback()

        data_insert.close()
        data_cursor.close()

    def create_table(self):

        table_cursor = self.source.connection.cursor()
        table_cursor.execute("SHOW CREATE TABLE " + self.table)

        for column in table_cursor:
            insert_cursor = self.destination.connection.cursor()
            insert_cursor.execute(column[1])
            insert_cursor.close()
        table_cursor.close()

    def check_table(self):
        exists = self.table_exists()

        if exists is True and self.replace is False:
            raise ValueError("The table " + self.table + " exists.  "
                             "If you want to replace it, set source.tables.create.replace to true in your config file.")

        if exists:
            drop_cursor = self.destination.connection.cursor()
            drop_cursor.execute("SET foreign_key_checks = 0;")
            drop_cursor.execute("DROP TABLE " + self.table)
            drop_cursor.execute("SET foreign_key_checks = 1;")
            drop_cursor.close()

    def table_exists(self):
        try:
            db = self.destination.connection.cursor()
            db.execute("DESCRIBE " + self.table)

            if db.fetchall() > 0:
                db.close()
                return True
        except MySQLdb.ProgrammingError:
            db.close()
            return False

    def get_insert_base(self):
        columns = self.get_column_string()
        string = "INSERT INTO " + self.table + "(" + columns["columns"] + ") Values (" + columns["values"] + ")"
        return string

    def get_column_string(self):
        output = []
        values = []
        db = self.source.connection.cursor()
        db.execute("DESCRIBE " + self.table)

        for column in db:
            try:
                if column.index("PRI"):
                    self.primary = column[0]
            except ValueError as e:
                # We know most columns won't be PRI.
                e.message

            value = self.get_column_type(column[1])
            if value is not False:
                self.columns.append(column[0])
                values.append(value)

        return {"columns": ",".join(output), "values": ",".join(values)}

    def build_query(self):
        qry_string = ["SELECT * FROM", self.table]

        if self.source.config["tables"]["order"] is not False:
            order_by = "ORDER BY " + self.primary + " " + self.source.config["tables"]["order"]
            qry_string.append(order_by)

        if self.source.config["tables"]["all"] is False:
            qry_string.append("LIMIT 0, " + str(self.source.config["tables"]["records"]))

        return " ".join(qry_string)

    # @TODO Make this static.
    def get_insert_record(self, record):
        data = []
        for field in record:
            if isinstance(field, long):
                field = str(field)
            data.append(field)

        return data

    # @TODO Make this static.
    def get_column_type(self, column):
        string = re.split(r'\W+', column)

        types = {
            "varchar": "%s",
            "timestamp": "%s",
            "int": "%s",
            "tinyint": "%s",
            "text": "%s",
            "longtext": "%s"
        }

        return types.get(string[0], "Type not found.")
