create table planets (name text, moons int8);

insert into planets (name, moons) values ('Earth', 1);
begin;
insert into planets (name, moons) values ('Mars', 2);
select name, moons from planets order by name asc;
commit;

drop table planets;
