#include <iostream>
#include <fstream>

#include "server.h"
#include "storage.h"

int main(int argc, char** argv) {
    wsldb::Storage::DropDatabase("/tmp/testdb"); //dropping database everytime to simplify testing
    wsldb::Storage storage("/tmp/testdb");

    wsldb::Server server(storage);
    server.Listen("3000");

    /*
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
        int listener_fd = server.GetListenerFD("3000");
        server.ListenAndSpawnClientHandlers(listener_fd);
    }*/


    return 0;
}
