create table planets (system text, name text, unique(system, name) nulls not distinct);

insert into planets (system, name) value ('Sol', 'Earth');
delete from planets where system = 'Sol';
insert into planets (system, name) value ('Sol', 'Earth'), ('Sirius', 'A');
insert into planets (system, name) value ('Sol', 'Mars');

select _rowid, system, name from planets;
