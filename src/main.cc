#include <cassert>
#include <iostream>
#include <vector>

#include "db.h"

int main(int argc, char** argv) {
    wsldb::DB db("/tmp/testdb");

    if (argc > 1) {
        db.ExecuteScript(argv[1]);
    } else {
        std::cout << "Currently requires input sql file" << std::endl;
    }

    return 0;
}
