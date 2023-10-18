create table planets (id int4, name text, rings bool, primary key (id));
insert into planets (id, name, rings) values (1, 'Earth', false), (2, 'Saturn', true), (3, 'Jupiter' true);
select id, name, rings from planets where id = 3;
select id, name, rings from planets where name = 'Saturn';
select id, name, rings from planets where rings = false;

select id, name, rings from planets where rings <> true;
select id, name, rings from planets where name <> 'Earth';
select id, name, rings from planets where id <> 1;


drop table planets;
