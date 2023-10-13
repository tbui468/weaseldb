#pragma once

#include <string>

#include "status.h"
#include "storage.h"
#include "./../include/tcp.h"

#define BACKLOG 10

namespace wsldb {

struct ConnHandlerArgs {
    Storage* storage;
    int conn_fd;
};

class Server: public TCPEndPoint {
public:
    Server(Storage& storage): listener_fd_(-1), storage_(storage) {}

    virtual ~Server() {
        close(listener_fd_);
    }

    void Listen(const char* port);
private:
    int GetListenerFD(const char* port);
    static void SigChildHandler(int s);
    static void ConnHandler(ConnHandlerArgs* args);
    static std::string PreparePacket(char type, const std::string& msg);
private:
    int listener_fd_;
    Storage storage_;
};

}
