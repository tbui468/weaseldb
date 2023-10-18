create table planets (id int4, name text, moons int4, primary key (id));
insert into planets (id, name, moons) values (1, 'Earth', 1), (2, 'Mars', 2), (3, 'Venus', 0);

select count(id), count(name), count(moons) from planets;
select sum(id), sum(moons) from planets;
select max(id), max(name), max(moons) from planets;
select min(id), min(name), min(moons) from planets;
select avg(id) from planets;

drop table planets;
