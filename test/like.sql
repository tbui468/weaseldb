create table planets (name text, primary key(name));
insert into planets (name) values ('Earth'), ('Mars'), ('Mercury'), ('Venus');
select name from planets where name like '%ar%';
select '----';
select name from planets where name like '%s';
select '----';
select name from planets where name like 'M%';
select '----';
select name from planets where name like '%r____';
drop table planets;