create table planets (name text, moons int8);
insert into planets (name, moons) values ('Mars', 2);

select name, moons from planets;
describe table planets;

drop table planets;
