drop table if exists planets;
create table planets (id int4, name text, rings bool, primary key (id));
insert into planets (id, name, rings) values (1, 'Earth', false), (2, 'Saturn', true), (3, 'Jupiter' true);
insert into planets (id, name, rings) values (4, 'Uranus', true), (5, 'Neptune', true);
select _rowid, id, name, rings from planets;
drop table planets;
