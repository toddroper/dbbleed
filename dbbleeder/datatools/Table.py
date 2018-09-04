import re
import MySQLdb
from datetime import datetime
from pprint import pprint


class Table:

    columns = []
    values = []
    primary = False
    records = 0
    batch_size = 100

    def __init__(self, table, mode, source, destination, create, replace):
        self.table = table
        self.mode = mode
        self.source = source
        self.destination = destination
        self.create = create
        self.replace = replace
        self.insert_stmt = self.get_insert_base()

        # Turn off foreign key checks
        self.destination.toggle_fk(True)

        if self.mode == "db":
            self.batch_size = self.source.config["tables"]["records"]
        else:
            self.batch_size = self.source.config["tables"]["batch_size"]

    def build_table(self):
        self.check_table()
        self.create_table()

    def copy_data(self, start=0):
        data_cursor = self.source.connection.cursor()
        data_insert = self.destination.connection.cursor()
        query = self.build_query(start)

        data_cursor.execute(query)

        try:
            inserts = []
            for record in data_cursor:
                data = self.get_insert_record(record)
                inserts.append(data)
            data_insert.executemany(self.insert_stmt, inserts)

            self.destination.connection.commit()
        except MySQLdb.Error as error:
            error.message

        data_insert.close()
        data_cursor.close()
        batch_data = "Copied records " + str(start) + " to " + str(start + self.batch_size)
        self.records += self.batch_size
        return batch_data

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
            drop_cursor.execute("DROP TABLE " + self.table)
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

        return "INSERT INTO " + self.table + "(" + columns["columns"] + ") Values (" + columns["values"] + ")"

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
                output.append(column[0])
                values.append(value)

        return {"columns": ",".join(output), "values": ",".join(values)}

    def build_query(self, start):
        qry_string = ["SELECT * FROM", self.table, self.get_where()]

        if 'order' in self.source.config["tables"] and self.source.config["tables"]["order"] is not False:
            order_by = "ORDER BY " + self.primary + " " + self.source.config["tables"]["order"]
            qry_string.append(order_by)

        limit = self.get_limit(start)
        if start > 0:
            start += 1

        qry_string.append("LIMIT " + str(start) + "," + limit)

        return " ".join(qry_string)

    def get_limit(self, start):
        return str(start + self.batch_size)

    def get_where(self):
        where = ''
        if 'where' in self.source.config["tables"]:
            where = "WHERE " + self.source.config["tables"]["where"]
        return where

    def get_row_count(self):
        db = self.source.connection.cursor()
        string = "SELECT COUNT(*) FROM " + self.table + " " + self.get_where()
        db.execute(string)

        return int(db.fetchall()[0][0])

    # @TODO Make this static.
    def get_insert_record(self, record):
        data = []
        for field in record:
            if isinstance(field, long) or isinstance(field, int):
                field = str(field)

            if type(field) == datetime:
                field = field.strftime("%Y-%m-%d %H:%M:%S")

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
            "bigint": "%s",
            "text": "%s",
            "longtext": "%s"
        }

        return types.get(string[0], "Type not found.")
