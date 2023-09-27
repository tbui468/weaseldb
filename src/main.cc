#include <cassert>
#include <iostream>
#include <vector>
#include <fstream>

#include "server.h"
#include "storage.h"

int main(int argc, char** argv) {
    wsldb::Server server;

    wsldb::Storage storage("/tmp/testdb");

    if (argc > 1) {
        std::string line;
        std::string query;
        std::string path = argv[1];
        std::ifstream script(path);
        while (getline(script, line)) {
            query += line;
        }
        script.close();

        server.RunQuery(query, &storage);
    } else {
        std::cout << "Currently requires input sql file" << std::endl;
    }

    return 0;
}
