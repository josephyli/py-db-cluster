import pymysql.cursors
import sys
import os
from ConfigParser import SafeConfigParser
from StringIO import StringIO

configfilename = sys.argv[1]
if os.path.isfile(configfilename):
    # read file into a string
    with open(configfilename) as stream:
        stream = StringIO("[fakesection]\n" + stream.read())
        
        cp = SafeConfigParser()
        cp.readfp(stream) 
        
        driver=cp.get('fakesection', 'catalog.driver') 
        hostname=cp.get('fakesection', 'catalog.hostname') 
        username=cp.get('fakesection', 'catalog.username') 
        passwd=cp.get('fakesection', 'catalog.passwd') 
        numnodes=cp.get('fakesection', 'numnodes') 


        for node in ['node1', 'node2']:
            for candidate in ['driver', 'hostname', 'username', 'passwd']:
                print '%s.%-12s  : %s' % ('fakesection', node + "." + candidate, cp.has_option('fakesection', node + "." + candidate))
            print

else:
    print("Config file not found")


# # Connect to the database
# connection = pymysql.connect(host='192.168.0.50',
#                              user='root',
#                              password='root',
#                              db='testdatabase',
#                              charset='utf8mb4',
#                              cursorclass=pymysql.cursors.DictCursor)
# try:
#     with connection.cursor() as cursor:
#         # Create a new record
#         sql = "CREATE TABLE Persons (PersonID int, LastName varchar(255));"
#         cursor.execute(sql)

#     # connection is not autocommit by default. So you must commit to save
#     # your changes.
#     connection.commit()

# finally:
#     connection.close()
