create table planets (id int8, name text, primary key (id));
insert into planets (id, name) values (1, 'Earth'), (2, 'Saturn'), (3, 'Jupiter');
update planets as p set p.name = 'Venus' where p.id = 1;
select id, name from planets;

drop table planets;
