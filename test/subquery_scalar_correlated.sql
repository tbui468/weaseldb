drop table if exists planets;

create table planets (name text, moons int4, rings bool, primary key (name));
insert into planets (name, moons, rings) values ('Earth', 1, false), ('Mars', 2, false), ('Saturn', 50, true), ('Jupiter', 40, true), ('Mercury', 0, false);
insert into planets (name, moons, rings) values ('Neptune', 8, true);

select distinct px.rings, (select sum(py.moons) from planets as py where py.rings = px.rings) from planets as px;

drop table planets;
