import thread
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
            self.name = self.config["source"]["table"]["name"]
        # Now run delegate ops for copying the db/table/data.
        if self.mode == "db":
            self.copy_tables()
        elif self.mode == "table":
            self.copy_table()
        else:
            self.copy_data()

    def copy_tables(self):
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

    def copy_data(self):
        if self.current_table is False:
            self.get_table()

    def create_table(self):
        if isinstance(self.name, str) or isinstance(self.name, unicode):
            name = str(self.name)
        else:
            name = str(self.name[0])

        print "Processing Table: " + name

        self.get_table(True)

    def get_table(self, build=False):
        self.current_table = Table(self.name, self.source, self.destination, self.create, self.replace)

        if build is True:
            self.current_table.build_table()

