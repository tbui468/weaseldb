create table planets (id int8, name text, rings bool, primary key (id));
insert into planets (id, name, rings) values (1, 'Earth', false), (9000000000, 'Saturn', true), (3, 'Jupiter' true);
select id, name, rings from planets;

drop table planets;
