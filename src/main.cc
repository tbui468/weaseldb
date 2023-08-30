#include <cassert>
#include <iostream>
#include <vector>

#include "db.h"

int main() {
    wsldb::DB db("/tmp/testdb");
//    db.ShowTables();
//    db.execute("create table planets (id int primary key, name text, moons int, rings bool);");
//    db.execute("insert into planets values (1, 'Saturn', 50, true), (2, 'Earth', 1, false), (3, 'Mars', 2, false);");

    //db.execute("select id, name, moons, rings from planets;");
    db.execute("select id, name, moons from planets where name = 'Mars';");
    db.execute("select id, name, moons from planets where name <> 'Saturn';");
    db.execute("select id, name, moons from planets where moons <= 2;");
    db.execute("select id, name, moons from planets where moons < 2;");
    db.execute("select id, name, moons from planets where moons >= 2;");
    db.execute("select id, name, moons from planets where moons > 2;");

    /*

    rocksdb::DB* table;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status;
    status = rocksdb::DB::Open(options, "/tmp/testdb/planets", &table);
    if (!status.ok())
        std::cout << "shit broke\n";
    else
        std::cout << "okay!\n";

    for (int i = 0; i < 5; i++) {
        std::string key;
        std::string value;
        key.append((char*)&i, sizeof(int));
        status = table->Get(rocksdb::ReadOptions(), key, &value);
        if (!status.ok()) {
            std::cout << "no key\n";
        } else {
            std::cout << "okay!" << value.size() << std::endl;
        }
    }*/

    //TODO: open /tmp/testdb/planets and read in data for keys (as integers) 1, 2, and 3
    //deserialize data and make sure that it shows up as above (1, 'Venus', 0), etc

}
