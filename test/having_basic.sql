create table planets (name text, moons int8, rings bool, primary key (name));
insert into planets (name, moons, rings) values ('Mars', 2, false), ('Saturn', 50, true), ('Jupiter', 40, true), ('Earth', 1, false), ('Uranus', 20, true);
select rings, sum(moons) from planets group by rings having sum(moons) > 50;
select rings from planets group by rings having sum(1) < 3;
drop table planets;
