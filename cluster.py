import argparse
import sys
import os
import pymysql.cursors
from ConfigParser import SafeConfigParser
from StringIO import StringIO

def readConfig(configfilename):
    configFile = {}

    if os.path.isfile(configfilename):
        # open file
        with open(configfilename) as stream:
            # pass into string & add a header
            stream = StringIO("[fakesection]\n" + stream.read())

            # read/parse catalog data
            cp = SafeConfigParser()
            cp.readfp(stream)
            configFile['catalog.driver'] = cp.get('fakesection', 'catalog.driver')
            configFile['catalog.hostname'] = cp.get('fakesection', 'catalog.hostname')
            configFile['catalog.username'] = cp.get('fakesection', 'catalog.username')
            configFile['catalog.passwd'] = cp.get('fakesection', 'catalog.passwd')

            # read the number of nodes
            numnodes=cp.getint('fakesection', 'numnodes')

            # read node data and print out info
            for node in range(1, numnodes + 1):
                for candidate in ['driver', 'hostname', 'username', 'passwd']:
                    # test if candidate exists before adding to dictionary
                    if cp.has_option('fakesection', "node" + str(node) + "." + candidate):
                        # print cp.get('fakesection', "node" + str(node) + "." + candidate)
                        configFile["node" + str(node) + "." + candidate] = cp.get('fakesection', "node" + str(node) + "." + candidate)
                    else:
                        print "error: candidate not found"

        return configFile
    else:
        print("Config file not found")
        return null

def connect():
    connection = pymysql.connect(host='192.168.0.50',
                                 user='root',
                                 password='root',
                                 db='testdatabase',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "CREATE TABLE Persons (PersonID int, LastName varchar(255));"
            cursor.execute(sql)

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

    finally:
        connection.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("configfile", help="Location of Config File, See the README for more information")
    #parser.add_argument("-i", action="store_true", help="interactively rename files")
    args = parser.parse_args()
    configDict = readConfig(args.configfile)
    # print out the config dict
    print configDict
    #connect(configDict)

if __name__ == "__main__":
    main()
