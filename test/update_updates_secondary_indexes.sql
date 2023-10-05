create table planets (system text, name text, unique(system, name) nulls not distinct);

insert into planets (system, name) value ('Sol', 'Earth');
update planets set system = 'Sirius' where system = 'Sol';
insert into planets (system, name) value ('Sol', 'Earth');

select _rowid, system, name from planets;
