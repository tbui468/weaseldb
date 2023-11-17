#include "server.h"
#include "storage.h"
#include "inference.h"


int main(int argc, char** argv) {
    wsldb::Inference inference("/tmp/models");

    /*
    std::vector<int> results = inf->Predict();
    for (int i: results) {
        std::cout << i;
    }
    */

    wsldb::Storage::DropDatabase("/tmp/testdb");
    wsldb::Storage::CreateDatabase("/tmp/testdb");
    wsldb::Storage storage("/tmp/testdb");

    wsldb::Server server(&storage, &inference);
    server.Listen("3000");


    return 0;
}
