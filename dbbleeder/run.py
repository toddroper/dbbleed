from optparse import OptionParser
from dbbleeder.commands.preflight import dsconfig

parser = OptionParser()
parser.add_option("-c", "--config", dest="config", default="config.json",
                  help="The config file to use for the transition", metavar="CONFIG")
parser.add_option("-t", "--threads", dest="threads", default=4, help="Number of concurrent threads to use for process")

(options, args) = parser.parse_args()


def main():
    bleeder = dsconfig(options)
    bleeder.copy()


if __name__ == '__main__':
    main()
