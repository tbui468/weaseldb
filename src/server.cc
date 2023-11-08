#include <unordered_map>
#include <thread>

#include "server.h"
#include "tokenizer.h"
#include "parser.h"
#include "rocksdb/db.h"
#include "executor.h"
#include "analyzer.h"

namespace wsldb {

void Server::SigChildHandler(int s) {
    s = s; //silence warning

    int saved_errno = errno;

    while (waitpid(-1, NULL, WNOHANG) > 0);

    errno = saved_errno;
}

void Server::ConnHandler(ConnHandlerArgs* args) {
    Storage* storage = args->storage;
    int conn_fd = args->conn_fd;
    free(args);

    //TODO: should hide lexer/parser inside of a Planner class (rather than calling it a Interpreter)

    while (true) {
        //read in message
        std::vector<Token> tokens = std::vector<Token>();
        {
            std::cout << "handling connection\n";
            std::string msg;
            if (!Recv(conn_fd, msg)) {
                break;
            }

            int len = *((int*)(msg.data() + sizeof(char)));
            std::string query = msg.substr(sizeof(char) + sizeof(int), len - sizeof(int));

            Tokenizer tokenizer(query);
            bool tokenizer_failed = false;
            do {
                Token t;
                Status s = tokenizer.NextToken(&t);
                if (!s.Ok()) {
                    std::string buf = PreparePacket('E', s.Msg());
                    Send(conn_fd, buf);
                    std::string buf2 = PreparePacket('Z', ""); //ready for query
                    Send(conn_fd, buf2);
                    tokenizer_failed = true;
                    break;
                }
                tokens.push_back(t);
            } while (tokens.back().type != TokenType::Eof);
            if (tokenizer_failed) continue;
        }

        std::vector<Txn> txns;
        {
            Parser parser(tokens);
            Status s = parser.ParseTxns(txns);
            if (!s.Ok()) {
                std::string buf = PreparePacket('E', s.Msg());
                Send(conn_fd, buf);
                std::string buf2 = PreparePacket('Z', "");
                Send(conn_fd, buf2);
                continue;
            }
        }

        for (Txn txn: txns) {
            Batch batch;
            Status s;
            Analyzer a(storage);
            Executor e(storage, &batch);

            for (Stmt* stmt: txn.stmts) {
                std::vector<DatumType> types;
                s = a.Verify(stmt, types);
                if (!s.Ok())
                    break;

                s = e.Execute(stmt);
                if (!s.Ok())
                    break;
            }

            if (s.Ok() && txn.commit_on_success)
                batch.Write(*storage);

            //TODO: if error, send 'E' + message and continue loop
            if (!s.Ok()) {
                std::string buf = PreparePacket('E', s.Msg());
                Send(conn_fd, buf);
                continue;
            }

            if (s.Ok()) {
                for (RowSet* rs: s.Tuples()) {
                    std::string row_description = rs->SerializeRowDescription();
                    std::string buf = PreparePacket('T', row_description);
                    Send(conn_fd, buf);

                    std::vector<std::string> data_rows = rs->SerializeDataRows();
                    for (const std::string& r: data_rows) {
                        std::string buf = PreparePacket('D', r);
                        Send(conn_fd, buf);
                    }
                }
            }

            std::string buf = PreparePacket('C', s.Msg());
            Send(conn_fd, buf);
        }

        {
            std::string buf = PreparePacket('Z', ""); //ready for query
            Send(conn_fd, buf);
        }

        
    }

    std::cout << "connection closed\n";

    close(conn_fd);
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
    sa.sa_handler = SigChildHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGCHLD, &sa, NULL) == -1) {
        exit(1);
    }

    return sockfd;
}

void Server::Listen(const char* port) {
    int listener_fd_ = GetListenerFD(port);

    while (true) {
        socklen_t sin_size = sizeof(struct sockaddr_storage);
        struct sockaddr_storage their_addr;
        int conn_fd = accept(listener_fd_, (struct sockaddr*)&their_addr, &sin_size);

        if (conn_fd == -1)
            continue;

        ConnHandlerArgs* args = new ConnHandlerArgs();
        args->storage = &storage_;
        args->conn_fd = conn_fd;

        std::thread thrd(ConnHandler, args);
        thrd.detach();
    } 
}

std::string Server::PreparePacket(char type, const std::string& msg) {
    std::string buf;

    buf.append((char*)&type, sizeof(char));

    int size = sizeof(int) + msg.size();
    buf.append((char*)&size, sizeof(int));

    buf += msg;

    return buf;
}

}
