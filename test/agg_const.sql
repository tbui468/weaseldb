create table planets (id int4, primary key (id));
insert into planets values (1), (2), (3);

select count(5);
select sum(5);
select min(0);
select max(0);
select '----';
select count(5) from planets;
select sum(5) from planets;
select min(0) from planets;
select max(0) from planets;
select avg(9) from planets;

drop table planets;
