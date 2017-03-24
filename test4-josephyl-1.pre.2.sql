create table Sailors (
  sid INT not null,
  name varchar(100),
  rating INT,
  age INT,
  primary key (sid));
insert into Sailors (sid, name, rating, age) values (1,'Jack Sparrow', 10, 40);
insert into Sailors (sid, name, rating, age) values (2,'Will Turner', 6, 26);
insert into Sailors (sid, name, rating, age) values (3,'Elizabeth Swann', 7, 24);
insert into Sailors (sid, name, rating, age) values (4,'Mr. Gibbs', 9, 51);
insert into Sailors (sid, name, rating, age) values (5,'Davey Jones', 10, 420);