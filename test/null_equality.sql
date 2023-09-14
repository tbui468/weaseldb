drop table if exists planets;

select null is null, null is not null;
select 1 is null, 1 is not null;
select 1.0 is null, 1.0 is not null;
select true is null, true is not null;
select false is null, false is not null;
select 'cat' is null, 'cat' is not null;

create table planets (name text, rings bool, primary key(name));
insert into planets (name, rings) values ('Earth', null), ('Mars', false);
select name from planets where rings is null;
select name from planets where rings is not null;
