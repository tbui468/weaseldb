#include <unordered_map>
#include <thread>

//C headers/defines for networking code
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#define BACKLOG 10

#include "server.h"
#include "tokenizer.h"
#include "parser.h"
#include "interpreter.h"
#include "rocksdb/db.h"

namespace wsldb {


void sigchld_handler(int s) {
    s = s; //silence warning

    int saved_errno = errno;

    while (waitpid(-1, NULL, WNOHANG) > 0);

    errno = saved_errno;
}


int Server::GetListenerFD(const char* port) {
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
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGCHLD, &sa, NULL) == -1) {
        exit(1);
    }

    return sockfd;
}

void tcp_send(int sockfd, char* buf, int len) {
    ssize_t written = 0;
    ssize_t n;

    while (written < len) {
        if ((n = send(sockfd, buf + written, len - written, 0)) <= 0) {
            if (n < 0 && errno == EINTR) //interrupted but not error, so we need to try again
                n = 0;
            else {
                exit(1); //real error
            }
        }

        written += n;
    }
}


bool tcp_recv(int sockfd, char* buf, int len) {
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

void handle_client_conn(int conn_fd) {
    //TODO: start here
    //handle request here (look at vaquita/server/src/main.c for code that handles clients)
    while (true) {
        int32_t request_len;
        if (!tcp_recv(conn_fd, (char*)&request_len, sizeof(int32_t))) {
            printf("client disconnected\n");
            break;
        }

        //echo back request_len to test
        std::cout << "client number: " << request_len << std::endl;
        tcp_send(conn_fd, (char*)&request_len, sizeof(int32_t));
        break;

        /*
        char buf[request_len + 1];
        if (!tcp_recv(conn_fd, buf, request_len)) {
            printf("client disconnected\n");
            break;
        }

        buf[request_len] = '\0';

        //allocate buffer for response sent back to client
        //execute query, and put response into buffer
        //send buffer back (be sure to prepend with buffer size)

        c->output->count = 0;
        uint32_t count = c->output->count;
        vdbbytelist_append_bytes(c->output, (uint8_t*)&count, sizeof(uint32_t)); //saving space for length

        bool end = vdbserver_execute_query(&h, buf, c->output);
        *((uint32_t*)(c->output->values)) = (uint32_t)(c->output->count); //filling in bytelist length

        tcp_send(c->conn_fd, (char*)(c->output->values), sizeof(uint32_t));
        tcp_send(c->conn_fd, (char*)(c->output->values + sizeof(uint32_t)), c->output->count - sizeof(uint32_t));

        if (end) {
            //printf("client released database handle\n");
            break;
        }*/
    }
    

    close(conn_fd);
}

void Server::ListenAndSpawnClientHandlers(int listener_fd) {
    while (true) {
        socklen_t sin_size = sizeof(struct sockaddr_storage);
        struct sockaddr_storage their_addr;
        int conn_fd = accept(listener_fd, (struct sockaddr*)&their_addr, &sin_size);

        if (conn_fd == -1)
            continue;

        std::thread thrd(handle_client_conn, conn_fd);
        thrd.detach();
    } 
}

//TODO: Sever shouldn't be doing this - should spawn a backend process to handle query
Status Server::RunQuery(const std::string& query, Storage* storage) {
    Tokenizer tokenizer(query);
    std::vector<Token> tokens = std::vector<Token>();

    do {
        tokens.push_back(tokenizer.NextToken());
    } while (tokens.back().type != TokenType::Eof);

    Parser parser(tokens);
    std::vector<Txn> txns = parser.ParseTxns();

    Interpreter interp(storage);

    for (Txn txn: txns) {
        Status s = interp.ExecuteTxn(txn);
    }

    return Status(true, "ok");
}

}
