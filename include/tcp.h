#pragma once

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>

#include <iostream>

namespace wsldb {

class TCPEndPoint {
public:
    static void Send(int sockfd, const std::string& buf) {
        size_t written = 0;
        size_t n;

        while (written < buf.size()) {
            if ((n = send(sockfd, buf.data() + written, buf.size() - written, 0)) <= 0) {
                if (n < 0 && errno == EINTR) //interrupted but not error, so we need to try again
                    n = 0;
                else {
                    exit(1); //real error
                }
            }

            written += n;
        }
    }

    static bool Recv(int sockfd, std::string& buf) {
        int header_size = sizeof(char) + sizeof(int);
        char header[header_size];
        if (!RecvBytes(sockfd, header, header_size)) {
            return false;
        }

        //char type = *((char*)(header)); //Not used now
        //type = type; //keeping compiler warnings quiet for now
        int size = *((int*)(header + sizeof(char)));
        int remaining_size = size - sizeof(int);

        buf.append(header, header_size);

        char payload[remaining_size];
        if (!RecvBytes(sockfd, payload, remaining_size)) {
            return false;
        }

        buf.append(payload, remaining_size);

        return true;
    }
private:
    static bool RecvBytes(int sockfd, char* buf, int len) {
        ssize_t nread = 0;
        ssize_t n;
        while (nread < len) {
            if ((n = recv(sockfd, buf + nread, len - nread, 0)) < 0) {
                if (n < 0 && errno == EINTR)
                    n = 0;
                else
                    exit(1);
            } else if (n == 0) {
                //connection ended
                return false;
            }

            nread += n;
        }

        return true;
    }
};

}
