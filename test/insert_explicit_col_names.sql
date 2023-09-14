drop table if exists planets;

create table planets (name text, moons int4, rings bool, primary key (name));
insert into planets (moons, rings, name) values (1, false, 'Earth'), (2, false, 'Mars');
insert into planets (rings, name, moons) values (true, 'Saturn', 50);
select name, moons, rings from planets;

drop table planets;
