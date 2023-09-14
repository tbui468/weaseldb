drop table if exists planets;

create table planets (id int4, name text, mass float4, rings bool, primary key (id));
insert into planets (id, name, mass, rings) values (1, 'Earth', 3.232, false), (2, 'Mars', .2320, true), (3, 'Venus', 2.30, true);

select p.name, p.id, name, p.mass, p.rings from planets as p;

drop table planets;
