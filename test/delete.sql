create table planets (id int4, name text, rings bool, primary key (id));
insert into planets (id, name, rings) values (1, 'Earth', false), (2, 'Saturn', true), (3, 'Jupiter' true);
delete from planets where id = 2;
select id, name, rings from planets;

drop table planets;
