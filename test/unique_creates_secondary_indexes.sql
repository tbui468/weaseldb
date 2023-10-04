create table planets (system text, name text, unique(system, name) nulls not distinct);
describe table planets;

create table moons (name text, moons int8, unique(name) nulls not distinct);
describe table moons;

create table moons2 (name text, moons int8, primary key (name, moons), unique(name) nulls not distinct);
describe table moons2;
