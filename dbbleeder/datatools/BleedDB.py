import MySQLdb as mysql


class BleedDB:

    config = False
    tables = False

    def __init__(self, config, source=False):
        self.config = config
        self.source = source

        self.connection = mysql.connect(host=config["host"], user=config["user"], passwd=config["pass"],
                                        db=config['name'])
        self.get_tables()

    def get_tables(self):
        if self.source is False or (self.source is True and self.config["tables"]["all"]):
            cursor = self.connection.cursor()
            cursor.execute("show tables")
            self.tables = cursor.fetchall()
            cursor.close()
        else:
            self.tables = self.config["tables"]["get"]
