create table planets (name text, moons int8);
insert into planets (name, moons) values ('Earth', 1), ('Mars', 2), ('Venus', 3);

select max(moons) + min(moons) from planets;
select min(moons) + 10 from planets;

drop table planets;
