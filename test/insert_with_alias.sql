create table planets (id int8, name text, rings bool, primary key (id));
insert into planets as p (p.id, p.name, p.rings) values (1, 'Earth', false), (2, 'Saturn', true), (3, 'Jupiter' true);
select _rowid, name, rings from planets;

drop table planets;
