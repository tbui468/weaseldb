create table pets (id int4, name text, mass float4, primary key (id));
insert into pets (id, name, mass) values (1, 'cat', 30.0), (2, 'bat', 10.0), (3, 'ant' 20.0);

select id, name from pets order by id asc;
select id, name from pets order by id desc;
select id, name from pets order by name asc;
select id, name from pets order by name desc;
select id, name from pets order by mass asc;
select id, name from pets order by mass desc;

drop table pets;
