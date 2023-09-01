drop table if exists planets;
drop table if exists moons;
create table planets (id int, name text, moons int, rings bool, primary key (id, moons));
insert into planets values (1, 'Earth', 3, false), (2, 'Mars', 2, false), (3, 'Saturn', 2, true);
update planets set rings = false where name = 'Saturn';
select id, name, moons, rings from planets order by id asc, moons desc limit 2;
select count(moons) from planets;
select min(id) from planets order by moons asc;
select max(id) from planets;

describe table planets;
