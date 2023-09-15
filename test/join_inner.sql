drop table if exists planets;
drop table if exists moons;

create table planets (name text, moons int4, primary key (name));
insert into planets (name, moons) values ('Earth', 1), ('Mars', 2);

create table moons (name text, planet text, primary key(name));
insert into moons (name, planet) values ('Luna', 'Earth'), ('Deimos', 'Mars'), ('Phobos', 'Mars');

select p.name, p.moons, m.name from planets as p inner join moons as m on p.name = m.planet;

drop table planets;
drop table moons;
