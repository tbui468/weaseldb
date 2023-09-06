drop table if exists pets;

create table pets (id int4, name text, mass float4, primary key (id));
insert into pets values (1, 'cat', 30.0), (2, 'ant', 10.0), (3, 'cat' 20.0);

select id, name, mass from pets limit 0;
select '----';
select id, name, mass from pets limit 1;
select '----';
select id, name, mass from pets limit 2;
select '----';
select id, name, mass from pets limit 3;
select '----';
select id, name, mass from pets limit 4;
