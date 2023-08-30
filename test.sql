create table planets (id int primary key, name text, moons int, rings bool);
insert into planets values (1, 'Saturn', 50, true), (2, 'Earth', 1, false), (3, 'Mars', 2, false);
select moons, name from planets;
select name from planets where moons >= 2;
