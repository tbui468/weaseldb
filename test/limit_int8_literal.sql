drop table if exists planets;
create table planets (id int4, name text, rings bool, primary key (id));
insert into planets (id, name, rings) values (1, 'Earth', false), (2, 'Saturn', true), (3, 'Jupiter' true);
select id, name, rings from planets limit 90000000000;
drop table planets;
