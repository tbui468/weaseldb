drop table if exists planets;
create table planets (id int primary key, name text, moons int , rings bool);
insert into planets values (1, 'Earth', 1, false), (2, 'Mars', 2, false), (3, 'Saturn', 50, true);
select id, name, moons, rings from planets;
