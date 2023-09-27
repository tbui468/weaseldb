#pragma once

#include <string>
#include "status.h"
#include "storage.h"

namespace wsldb {

class Server {
public:
    Status RunQuery(const std::string& query, Storage* storage);
private:
};

}
