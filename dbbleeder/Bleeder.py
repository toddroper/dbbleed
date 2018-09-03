from threading import Thread, ThreadError
from dbbleeder.datatools.BleedDB import BleedDB
from dbbleeder.datatools.Table import Table


class Bleeder:
    mode = False
    current_table = False
    name = False

    def __init__(self, config):
        self.config = config
        self.create = self.config["create_table"]
        self.replace = self.config["replace_table"]
        self.source = BleedDB(config["source"], True)
        self.destination = BleedDB(config["destination"])

    def boot(self):
        # If we aren't doing a DB operation, set the table name.
        if self.mode != "db":
            self.name = self.config["source"]["tables"]["name"]
        # Now run delegate ops for copying the db/table/data.
        if self.mode == "db":
            self.copy_tables()
        elif self.mode == "table":
            self.copy_table()
        else:
            self.get_table()
            self.copy_data()

        # Turn foreign key checks back on
        self.destination.toggle_fk()

    def copy_tables(self):
        self.source.get_tables()
        for table in self.source.tables:
            self.name = table
            if self.create is not False:
                self.create_table()
            if self.config["structure_only"] is False:
                self.copy_data()
            # once we're done, clear out the current_table and name.
            self.current_table = False
            self.name = False

    def copy_table(self):
        self.get_table(self.create)
        rows = self.current_table.get_row_count()
        batch_size = self.current_table.batch_size
        count = 0
        threads = 0
        bleeder = self

        while rows > 0:
            if threads < 4:
                name = "Thread" + str(threads)
                Thread(target=bleeder.copy_data(count), name=name)
                count += batch_size
                rows -= batch_size
            print "Count:"
            print count
            print rows

    def copy_data(self, start=0):
        print self.current_table.copy_data(start)

    def create_table(self):
        if isinstance(self.name, str) or isinstance(self.name, unicode):
            name = str(self.name)
        else:
            name = str(self.name[0])

        print "Processing Table: " + name

        self.get_table(True)

    def get_table(self, build=False):
        self.current_table = Table(self.name, self.mode, self.source, self.destination, self.create, self.replace)

        if build is True:
            self.current_table.build_table()
