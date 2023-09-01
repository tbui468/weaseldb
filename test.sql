drop table if exists planets;
drop table if exists moons;
create table planets (id int, name text, moons int, rings bool, primary key (id, moons));
insert into planets values (1, 'Earth', 3, false), (2, 'Mars', 2, false), (3, 'Saturn', 2, true);
select id, rings, name from planets order by moons asc, id asc;
select id, rings, name from planets order by moons asc, id desc;

describe table planets;
