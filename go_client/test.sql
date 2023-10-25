create table planets (id int8, name text, primary key (id));
insert into planets (id, name) values (1, 'Earth'), (2, 'Mars');

create table moons (id int8, name text, primary key(id));
insert into moons (id, name) values (1, 'Luna'), (2, 'Deimos'), (3, 'Phobos');

select '------';
select p.id, p.name, m.id, m.name from planets as p cross join moons as m order by m.id asc, p.id asc;

drop table planets;
drop table moons;
