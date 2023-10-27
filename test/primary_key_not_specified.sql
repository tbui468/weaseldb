create table planets (name text, moons int8);
insert into planets (name, moons) values ('Mars', 2);
insert into planets (name, moons) values ('Mars', 2);
insert into planets (name, moons) values ('Mars', 2);

select _rowid, name, moons from planets;

drop table planets;
