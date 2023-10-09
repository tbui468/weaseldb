#include <iostream>
#include <cstring>
#include <thread>

#include "../include/tcp.h"

#define BACKLOG 10

namespace wsldb {

class Server: public TCPEndPoint {
public:
    static void SigChildHandler(int s) {
        s = s; //silence warning

        int saved_errno = errno;

        while (waitpid(-1, NULL, WNOHANG) > 0);

        errno = saved_errno;
    }

    static void ConnHandler(int conn_fd) {
        //should spawn an interpreter here...?

        while (true) {
            //read in message
            {
                std::string msg;
                if (!Recv(conn_fd, msg)) {
                    break;
                }
                int len = *((int*)(msg.data() + sizeof(char)));
                std::string query = msg.substr(sizeof(char) + sizeof(int), len - sizeof(int));
                std::cout << query << std::endl;
                //TODO: tokenize query
                //parse that fella
                //interpret the AST
                //get results, and return them in an appropriate format
            }

            //TODO: if select query, send row description 
            //then iterate over data rows and send them one at a time here

            //if error occured, send here also

            //sending complete response (temporary)
            {
                std::string out = "Transaction complete";

                char type = 'C'; //complete 
                int size = sizeof(int) + out.size();

                std::string buf;
                buf.append((char*)&type, sizeof(char));
                buf.append((char*)&size, sizeof(int));
                buf += out;

                Send(conn_fd, buf);
            }

            //ready for next query response
            {
                char type = 'Z'; //ready for query
                int size = sizeof(int);
                std::string buf;
                buf.append((char*)&type, sizeof(char));
                buf.append((char*)&size, sizeof(int));
                Send(conn_fd, buf);
            }

            
        }

        std::cout << "connection closed\n";

        close(conn_fd);
    }

    int GetListenerFD(const char* port) {
        struct addrinfo hints;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE; //use local ip address

        struct addrinfo* servinfo;
        if (getaddrinfo(NULL, port, &hints, &servinfo) != 0) {
            return 1;
        }

        //loop through results for getaddrinfo and bind to first one we can
        struct addrinfo* p;
        int sockfd;
        int yes = 1;
        for (p = servinfo; p != NULL; p = p->ai_next) {
            if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
                continue;

            if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1)
                exit(1);

            if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
                close(sockfd);
                continue;
            }

            break;
        }

        freeaddrinfo(servinfo);

        //failed to bind
        if (p == NULL) {
            exit(1);
        }

        if (listen(sockfd, BACKLOG) == -1) {
            exit(1);
        }

        //what was this for again?
        struct sigaction sa;
        sa.sa_handler = SigChildHandler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_RESTART;
        if (sigaction(SIGCHLD, &sa, NULL) == -1) {
            exit(1);
        }

        return sockfd;
    }

    virtual ~Server() {
        close(listener_fd_);
    }

    void Listen(const char* port) {
        int listener_fd_ = GetListenerFD(port);

        while (true) {
            socklen_t sin_size = sizeof(struct sockaddr_storage);
            struct sockaddr_storage their_addr;
            int conn_fd = accept(listener_fd_, (struct sockaddr*)&their_addr, &sin_size);

            if (conn_fd == -1)
                continue;

            std::thread thrd(ConnHandler, conn_fd);
            thrd.detach();
        } 
    }

private:
    int listener_fd_;
};

}

int main(int argc, char** argv) {
    wsldb::Server server;
    server.Listen("3000");
}
