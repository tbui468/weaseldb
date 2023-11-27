create table planets (name text, moons int8, rings bool, primary key (name));
insert into planets (name, moons, rings) values ('Mars', 2, false), ('Saturn', 50, true), ('Jupiter', 40, true), ('Earth', 1, false);
select rings, sum(moons) from planets group by rings
drop table planets;
