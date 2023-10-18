create table planets (name text, moons int4, rings bool, primary key (name));
insert into planets (name, moons, rings) values ('Earth', 1, false), ('Mars', 2, false), ('Saturn', 50, true), ('Jupiter', 40, true);

select px.rings, (select sum(py.moons) from planets as py) from planets as px;
select rings, (select sum(py.moons) from planets as py) from planets;
select px.rings, (select sum(moons) from planets) from planets as px;

drop table planets;
