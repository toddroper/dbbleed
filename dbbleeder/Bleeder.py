import thread
from dbbleeder.datatools.BleedDB import BleedDB
from dbbleeder.datatools.CreateTable import CreateTable


class Bleeder:
    mode = False

    def __init__(self, config):
        self.config = config
        self.source = BleedDB(config["source"], True)
        self.destination = BleedDB(config["destination"])

    def boot(self):
        if self.mode == "db":
            self.copy_tables()

    def copy_tables(self):
        for table in self.source.tables:
            if self.source.config["tables"]["create"] is not False:
                self.create(table)

    def create(self, table):
        if isinstance(table, str) or isinstance(table, unicode):
            name = str(table)
        else:
            name = str(table[0])

        print "Processing Table: " + name

        dest_table = CreateTable(name, self.source, self.destination)
        dest_table.begin()
