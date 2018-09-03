import os
import json
from dbbleeder.Bleeder import Bleeder


def dsconfig(options):
    config = parseconfig(options.config)
    bleeder = Bleeder(config)

    return bleeder


def parseconfig(filename):
    configfile = os.getcwd() + "/" + filename

    print "Using config file: " + configfile

    if os.path.isfile(configfile):
        return json.load(open(configfile))
    elif os.path.isfile(filename):
        return json.load(open(filename))
