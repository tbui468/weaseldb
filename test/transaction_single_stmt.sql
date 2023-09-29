drop table if exists planets;

create table planets (id int4, name text, primary key (id));
insert into planets (id, name) values (1, 'Earth'), (2, 'Mars');
insert into planets (id, name) values (3, 'Jupiter'), (1, 'Saturn');
insert into planets (id, name) values (1, 'Jupiter'), (4, 'Saturn');
select id, name from planets;

drop table planets;
