#pragma once

#include <string>

#include "status.h"
#include "storage.h"
#include "inference.h"
#include "./../include/tcp.h"

#define BACKLOG 10

namespace wsldb {

struct ConnHandlerArgs {
    Storage* storage;
    Inference* inference;
    int conn_fd;
};

class Server: public TCPEndPoint {
public:
    Server(Storage* storage, Inference* inference): listener_fd_(-1), storage_(storage), inference_(inference) {}

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
    Storage* storage_;
    Inference* inference_;
};

}
