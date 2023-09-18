drop table if exists planets;

create table planets (name text, moons int4, primary key(name));
insert into planets (name, moons) values ('Earth', 1), ('Mars', 2);
select name, moons from planets;
