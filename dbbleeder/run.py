import time
from optparse import OptionParser
from dbbleeder.commands.preflight import dsconfig

parser = OptionParser()
parser.add_option("-c", "--config", dest="config", default="config.json",
                  help="REQUIRED: The config file to use for the transition", metavar="CONFIG")
parser.add_option("-t", "--threads", dest="threads", default=4, help="Number of concurrent threads to use for process")
parser.add_option("-m", "--mode", dest="mode", default="db",
                  help="The bleeder mode.  Options are db, tables, or records")

(options, args) = parser.parse_args()


def main():
    start = time.time()
    bleeder = dsconfig(options)
    bleeder.mode = options.mode
    bleeder.boot()
    end = time.time()
    print "Start: " + str(start)
    print "End: " + str(end)
    print "Seconds elapsed: " + str((end - start))


if __name__ == '__main__':
    main()
