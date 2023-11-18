create table planets (id int8, name text);

insert into planets (id, name) values (1, 'Earth'), (2, 'Mars'), (3, 'Venus');
select * from planets;

drop table planets;
