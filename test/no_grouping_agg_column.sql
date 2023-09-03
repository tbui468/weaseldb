create table planets (id int, name text, moons int, primary key (id));
insert into planets values (1, 'Earth', 1), (2, 'Mars', 2), (3, 'Venus', 0);

select count(id), count(name), count(moons) from planets;
select sum(id), sum(moons) from planets;
select max(id), max(name), max(moons) from planets;
select min(id), min(name), min(moons) from planets;

drop table planets;
