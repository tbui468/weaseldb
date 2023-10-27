create table planets (system text, name text, unique(system, name) nulls not distinct);
insert into planets (system, name) values ('Sol', 'Earth');
insert into planets (system, name) values ('Andromeda', 'Earth');
insert into planets (system, name) values ('Sol', 'Mars');
insert into planets (system, name) values ('Sol', 'Earth');
select system, name from planets order by system asc, name asc;

create table moons (name text, moons int8, unique(name) nulls not distinct);
insert into moons (name, moons) values ('Saturn', 1);
insert into moons (name, moons) values ('Jupiter', 1);
insert into moons (name, moons) values ('Jupiter', 2);
select name from moons order by name asc;

create table moons2 (name text, moons int8, primary key (name, moons), unique(name) nulls not distinct, unique(moons) nulls not distinct);
insert into moons2 (name, moons) values ('Earth', 1);
insert into moons2 (name, moons) values ('Mars', 1);
insert into moons2 (name, moons) values ('Earth', 2);
select name, moons from moons2;

drop table planets;
drop table moons;
drop table moons2;
