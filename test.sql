create table planets (id int primary key, name text, moons int, rings bool);
insert into planets values (1, 'Saturn', 50, true), (2, 'Earth', 1, false), (3, 'Mars', 2, false);
select name, 4 + 3 * 3, 'dog' = 'dog' or false from planets where moons >= 1 + 1;
