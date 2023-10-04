create table planets (name text, moons int4, primary key (name));
insert into planets (name, moons) values ('Earth', 1), ('Mars', 2), ('Venus', 0);

create table moons (name text, planet text, primary key(name));
insert into moons (name, planet) values ('Luna', 'Earth'), ('Deimos', 'Mars'), ('Phobos', 'Mars'), ('Titan', 'Jupiter'), ('Io', 'Jupiter');

select p.name, p.moons, m.name, m.planet from planets as p full join moons as m on p.name = m.planet;
