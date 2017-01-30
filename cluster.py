import pymysql.cursors
import sys
import os
from ConfigParser import SafeConfigParser
from StringIO import StringIO

# class Node:
#     "Common base for nodes"

#     def __init__(self):
#         self.
def readConfig():
    "Takes filename of config from commandline and parses config file"
    configfilename = sys.argv[1]

    # create an empty dictionary to store data
    dict = {}

    if os.path.isfile(configfilename):
        # open file
        with open(configfilename) as stream:
            # pass into string & add a header
            stream = StringIO("[fakesection]\n" + stream.read())
            
            # read/parse catalog data
            cp = SafeConfigParser()
            cp.readfp(stream) 
            dict['catalog.driver'] = cp.get('fakesection', 'catalog.driver') 
            dict['catalog.hostname'] = cp.get('fakesection', 'catalog.hostname') 
            dict['catalog.username'] = cp.get('fakesection', 'catalog.username') 
            dict ['catalog.passwd'] = cp.get('fakesection', 'catalog.passwd') 
            
            # read the number of nodes
            numnodes=cp.getint('fakesection', 'numnodes') 

            # read node data and print out info
            for node in range(1, numnodes + 1):
                for candidate in ['driver', 'hostname', 'username', 'passwd']:
                    # test if candidate exists before adding to dictionary
                    if cp.has_option('fakesection', "node" + str(node) + "." + candidate):
                        # print cp.get('fakesection', "node" + str(node) + "." + candidate)
                        dict["node" + str(node) + "." + candidate] = cp.get('fakesection', "node" + str(node) + "." + candidate)
                    else:
                        print "error: candidate not found"

            # print out key pair for testing
            print "Printing dictionary key pairs:"
            for key in dict:
                print "\t", key, "\t\t", dict[key] 
        return dict

    else:
        print("Config file not found")
        return null

def connect():
    "Connects to the database"
    # Connect to the database
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

readConfig()

