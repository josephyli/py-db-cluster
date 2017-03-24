create table Reserves (
	sid INT ,
	bid INT NOT NULL,
	day date,
	FOREIGN KEY (sid) REFERENCES Sailors (sid)
		);

insert into Reserves (sid, bid, day) values (1, 1, '2016-06-16');
insert into Reserves (sid, bid, day) values (1, 1, '1959-08-14');
insert into Reserves (sid, bid, day) values (2, 1, '1999-04-14');
insert into Reserves (sid, bid, day) values (2, 4, '1974-08-14');
insert into Reserves (sid, bid, day) values (3, 2, '2000-02-17');
insert into Reserves (sid, bid, day) values (3, 2, '1998-08-07');
insert into Reserves (sid, bid, day) values (3, 2, '2003-01-07');
insert into Reserves (sid, bid, day) values (5, 1, '2014-10-27');
insert into Reserves (sid, bid, day) values (5, 2, '2002-12-27');
insert into Reserves (sid, bid, day) values (5, 3, '2009-11-27');
insert into Reserves (sid, bid, day) values (5, 4, '2004-10-07');
insert into Reserves (sid, bid, day) values (5, 5, '2010-05-06');