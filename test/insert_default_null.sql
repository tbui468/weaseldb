create table planets (name text, moons int4, rings bool, primary key (name));
insert into planets (name, moons, rings) values ('Earth', 1, false), ('Mars', 2, false);
insert into planets (name, moons) values ('Jupiter', 3), ('Saturn', 50);
insert into planets (name) values ('Neptune');
select name, moons, rings from planets;

drop table planets;
