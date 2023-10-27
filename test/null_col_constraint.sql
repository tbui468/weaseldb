create table planets (name text not null, moons int8, rings bool not null, primary key (name));
insert into planets (name, moons, rings) values ('Earth', null, false);
insert into planets (name, moons, rings) values ('Saturn', 3, null);
insert into planets (name, moons, rings) values ('Mars', 2, false);
select name, moons, rings from planets order by name asc;

drop table planets;
