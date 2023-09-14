create table pets (id int4, name text, mass float4, primary key (id));
insert into pets (id, name, mass) values (1, 'cat', 30.0), (2, 'ant', 10.0), (3, 'cat' 20.0);

select id, name from pets order by name asc, id asc;
select id, name from pets order by name asc, id desc;
select id, name from pets order by name asc, mass asc;
select id, name from pets order by name asc, mass desc;
select '----';
select id, name from pets order by name desc, id asc;
select id, name from pets order by name desc, id desc;
select id, name from pets order by name desc, mass asc;
select id, name from pets order by name desc, mass desc;

drop table pets;
