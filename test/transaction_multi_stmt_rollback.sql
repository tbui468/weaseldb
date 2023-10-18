create table planets (id int4, name text, primary key (id));

begin;
insert into planets (id, name) values (1, 'Earth');
insert into planets (id, name) values (2, 'Mars');
commit;

begin;
insert into planets (id, name) values (1, 'Mercury');
insert into planets (id, name) values (3, 'Venus');
commit;

begin;
insert into planets (id, name) values (4, 'Neptune');
insert into planets (id, name) values (2, 'Uranus');
commit;

begin;
insert into planets (id, name) values (3, 'Saturn');
insert into planets (id, name) values (4, 'Jupiter');
rollback;

select id, name from planets;

drop table planets;
