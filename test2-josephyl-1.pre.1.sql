CREATE TABLE IF NOT EXISTS dtables (tname char(32), nodedriver char(64), nodeurl char(128), nodeuser char(16), nodepasswd char(16), partmtd int, nodeid int, partcol char(32), partparam1 char(32), partparam2 char(32));
INSERT INTO dtables VALUES ('restaurants', 'com.mysql.jdbc.Drive', 'jdbc:mysql://127.0.0.1:3306/josephyltest2_2','db2inst1','mypasswd', 2, 1, 5, 2, NULL);
INSERT INTO dtables VALUES ('restaurants', 'com.mysql.jdbc.Drive', 'jdbc:mysql://127.0.0.1:3306/josephyltest2_3','db2inst1','mypasswd', 2, 2, 5, 2, NULL);
