#pragma once

#include <string>
#include "status.h"
#include "storage.h"

namespace wsldb {

class Server {
public:
    Status RunQuery(const std::string& query, Storage* storage);
    int GetListenerFD(const char* port);
    void ListenAndSpawnClientHandlers(int listener_fd);
private:
};

}
