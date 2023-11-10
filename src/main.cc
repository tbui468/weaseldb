
#include "server.h"
#include "storage.h"

int main(int argc, char** argv) {
    wsldb::Storage::DropDatabase("/tmp/testdb");
    wsldb::Storage::CreateDatabase("/tmp/testdb");
    wsldb::Storage storage("/tmp/testdb");

    wsldb::Server server(storage);
    server.Listen("3000");


    return 0;
}
