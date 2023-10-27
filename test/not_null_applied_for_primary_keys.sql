create table planets(system text, name text, moons int8, primary key(system, name));

insert into planets (system, name, moons) values ('Sol', 'Earth', 1);
insert into planets (system, name, moons) values (null, 'Saturn', 2);
insert into planets (system, name, moons) values ('Sol', null, 3);
insert into planets (system, name, moons) values (null, null, 4);
insert into planets (system, name, moons) values ('Sol', 'Mars', 5);

select system, moons from planets order by moons asc;

drop table planets;
