#include <cassert>
#include <iostream>
#include <vector>

#include "db.h"

int main(int argc, char** argv) {
    wsldb::DB db("/tmp/testdb");

    if (argc > 1) {
        db.ExecuteScript(argv[1]);
    } else {
    //    db.ShowTables();
    //    db.execute("create table planets (id int primary key, name text, moons int, rings bool);");
    //    db.execute("insert into planets values (1, 'Saturn', 50, true), (2, 'Earth', 1, false), (3, 'Mars', 2, false);");

        /*
        db.execute("select moons, name from planets;");
        db.execute("select id, name, moons from planets where name = 'Mars';");
        db.execute("select id, name, moons from planets where name <> 'Saturn';");
        db.execute("select id, name, moons from planets where moons <= 2;");
        db.execute("select id, name, moons from planets where moons < 2;");
        db.execute("select id, name, moons from planets where moons >= 2;");
        db.execute("select id, name, moons from planets where moons > 2;");*/
    }

    return 0;
}
