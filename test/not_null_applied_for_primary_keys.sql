create table planets(system text, name text, moons int8, primary key(system, name));

describe table planets;

drop table planets;
