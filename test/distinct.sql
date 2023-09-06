drop table if exists planets;

create table planets (id int4, name text, mass float4, rings bool, primary key (id));
insert into planets values (1, 'Earth', 30.0, false), (2, 'Saturn', 10.0, true), (3, 'Jupiter', 20.0, true);

select distinct rings from planets;
select distinct rings, name from planets;
select distinct id from planets;
