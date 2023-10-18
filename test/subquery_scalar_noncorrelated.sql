create table planets (name text, moons int4, primary key (name));
insert into planets (name, moons) values ('Earth', 1), ('Mars', 2);

select name, moons from planets where name = (select 'Mars');
select '----';
select name, moons from planets where moons = (select min(moons) from planets);
select '----';
select moons, (select max(moons) from planets), name from planets;

drop table planets;
