import MySQLdb as MySQL


class BleedDB:

    config = False
    tables = False

    def __init__(self, config, source=False):
        self.config = config
        self.source = source

        self.connection = MySQL.connect(host=config["host"], user=config["user"], passwd=config["pass"],
                                        db=config['name'])

    def get_tables(self):
        if self.source is False or (self.source is True and self.config["tables"]["all"]):
            cursor = self.connection.cursor()
            cursor.execute("show tables")
            self.tables = cursor.fetchall()
            cursor.close()
        else:
            self.tables = self.config["tables"]["get"]

    def toggle_fk(self, disable=False):
        cursor = self.connection.cursor()
        on = (disable is False)
        print on
        cursor.execute("SET foreign_key_checks = " + str(on) + ";")
