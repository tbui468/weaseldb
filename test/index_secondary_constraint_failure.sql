create table planets (system text, name text, unique(system, name) nulls not distinct);

insert into planets (system, name) values ('Sol', 'Earth');
insert into planets (system, name) values ('Sol', 'Earth'), ('Sirius', 'A');
insert into planets (system, name) values ('Sol', 'Mars');

select system, name from planets;

drop table planets;
