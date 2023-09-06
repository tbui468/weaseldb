create table planets (id int4, name text, rings bool, primary key (id));
insert into planets values (1, 'Earth', false), (2, 'Saturn', true), (3, 'Jupiter' true);
select id, name from planets;
select name, id from planets;
select rings from planets;
drop table planets;
