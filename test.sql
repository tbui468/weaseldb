drop table if exists planets;
drop table if exists moons;
create table planets (id int, name text, moons int, rings bool, primary key (id, moons));
insert into planets values (3, 'Earth', 1, false), (3, 'Mars', 2, false), (3, 'Saturn', 50, true);
update planets set id = 1 where id = 3 and moons = 1;
select id, name, moons, rings from planets;

describe table planets;
