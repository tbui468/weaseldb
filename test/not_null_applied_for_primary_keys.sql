drop table if exists planets;

create table planets(system text, name text, moons int4, primary key(system, name));

describe table planets;
drop table planets;
