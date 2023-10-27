create table planets(system text, name text, unique(system, name) nulls not distinct);

insert into planets (system, name) values ('Sol', null);
insert into planets (system, name) values ('Sol', null);
insert into planets (system, name) values (null, 'Earth');
insert into planets (system, name) values (null, 'Earth');
insert into planets (system, name) values (null, null);
insert into planets (system, name) values (null, null);

select system, name from planets;

drop table planets;
