create table planets (id int4, name text, primary key (id));
insert into planets (id, name) values (1, 'Earth'), (2, 'Mars');

create table moons (id int4, name text, primary key(id));
insert into moons (id, name) values (1, 'Luna'), (2, 'Deimos'), (3, 'Phobos');

select p.id, p.name, m.id, m.name from planets as p cross join moons as m order by m.id asc, p.id asc;
select '----';
select m1.id, m1.name, m2.id, m2.name from moons as m1 cross join moons as m2;
select '----';
select m1.id, m1.name, m2.id, m2.name from planets as m1 cross join planets as m2;
