create table planets (name text not null, moons int8, rings bool not null, primary key (name));
insert into planets (name, moons, rings) values ('Earth', null, false), ('Mars', 2, false);
describe table planets;
select name, moons, rings from planets;

drop table planets;
