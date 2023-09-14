create table planets (id int4, name text, mass float4, primary key (id));
insert into planets (id, name, mass) values (1, 'Earth', 10.0), (2, 'Saturn', 20.0), (3, 'Jupiter' 30.0);

select id, name from planets where id < 2;
select id, name from planets where id <= 1;
select id, name from planets where id > 2;
select id, name from planets where id >= 3;

select id, name from planets where name < 'F';
select id, name from planets where name <= 'Earth';
select id, name from planets where name > 'K';
select id, name from planets where name >= 'Saturn';

select id, name from planets where mass < 10.5;
select id, name from planets where mass > 20.5;

drop table planets;
