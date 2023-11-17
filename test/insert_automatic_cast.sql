create table planets (moons int8, mass float4, time timestamp);

insert into planets (moons, mass, time) values (1.0, 1, '2000-1-1 00:00:00');
select moons, mass, time from planets;

drop table planets;
