#include <cassert>
#include <iostream>
#include <vector>

#include "db.h"

/*

class Datum {
public:

private:
    bool isnull_;
};

class DatumInt: public Datum {
public:
private:
};

class DatumText: public Datum {
public:
private:
};*/


int main() {
    wsldb::DB db("/tmp/testdb");
    db.ShowTables();
//    db.execute("create table planets (id int primary key, name text, moons int);");
    db.execute("insert into planets values (1, 'Venus', 0), (2, 'Earth', 1), (3, 'Mars', 2);");
    db.execute("insert into pets values (1, 'Venus', 0), (2, 'Earth', 1), (3, 'Mars', 2);");
//    db.execute("create table students (id int primary key, firstname text, lastname text);");
//    db.ShowTables();

    /*
    db.execute("select id, name, moons from planets;");
    db.execute("select name, moons from planets;");
    db.execute("select name from planets where name='Earth';");
    db.execute("select name from planets where moons < 2;");
    db.execute("drop table planets;");*/
    /*
    db.InsertRecord(1, 4, "dog");
    db.InsertRecord(2, 5, "cat");
    db.InsertRecord(3, 6, "turtle");
    struct WeaselRecord result;
    if (db.GetRecord(1, &result).ok()) {
        std::cout << result.v1 << '\n' << result.v2 << std::endl;
    } else {
        std::cout << "record with key not found" << std::endl;
    }

    std::vector<struct WeaselRecord> recs = db.IterateScan();

    for (const struct WeaselRecord& r: recs) {
        std::cout << r.v1 << ", " << r.v2 << std::endl;
    }*/

}
