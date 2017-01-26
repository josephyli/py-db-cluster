import pymysql.cursors

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
