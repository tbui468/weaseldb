#include <iostream>
#include <cstring>
#include <fstream>

#include "../include/tcp.h"
#include "../include/datum.h"

namespace wsldb {

class Client: public TCPEndPoint {
public:
    void Connect(const char* host, const char* port) {
        struct addrinfo hints;
        struct addrinfo* servinfo;
        memset(&hints, 0, sizeof(struct addrinfo));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        if (getaddrinfo(host, port, &hints, &servinfo) != 0)
            exit(1);

        struct addrinfo* p;

        //connect to first result possible
        for (p = servinfo; p != NULL; p = p->ai_next) {
            if ((sockfd_ = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
                continue;

            if (connect(sockfd_, p->ai_addr, p->ai_addrlen) == -1) {
                close(sockfd_);
                continue;
            }

            break;
        }

        if (p == NULL)
            exit(1);

        freeaddrinfo(servinfo);
    }

    virtual ~Client() {
        close(sockfd_);
    }

    void SendQuery(const std::string& query) {
        std::string msg;
        char type = 'Q';
        msg.append(&type, sizeof(char));
        int size = query.size() + sizeof(int);
        msg.append((char*)&size, sizeof(int));
        msg += query;

        Send(sockfd_, msg);
    }

    bool RecvResponse(std::string& buf) {
        return Recv(sockfd_, buf);
    }

    bool ProcessQuery(const std::string& query) {
        SendQuery(query);
        //TODO: if query is 'exit;', exit this loop so that client disconnects (or should be explicitly tell server we are disconnecting?)
        
        std::vector<DatumType> types;

        while (true) {
            std::string response;
            bool result = RecvResponse(response);
            char code = *((char*)(response.data()));
            int len = *((int*)(response.data() + sizeof(char)));
            std::string msg = response.substr(sizeof(char) + sizeof(int), len - sizeof(int));
            //read first byte
            switch (code) {
                case 'T': {
                    types.clear(); //some queries such as 'describe' return multiple rowsets (including row descriptions)
                    int off = 0;
                    int count = *((int*)(msg.data()));
                    off += sizeof(int);

                    for (int i = 0; i < count; i++) {
                        DatumType type = *((DatumType*)(msg.data() + off));
                        types.push_back(type);
                        off += sizeof(DatumType);
                        int name_size = *((int*)(msg.data() + off));
                        off += sizeof(int);
                        std::string name = msg.substr(off, name_size);
                        off += name_size;

                        //std::cout << name << ": " << Datum::TypeToString(type) << std::endl;
                    }

                    break;
                }
                case 'D': {
                    int off = 0;
                    for (DatumType type: types) {
                        bool is_null = *((bool*)(msg.data() + off));
                        off += sizeof(bool);
                        if (is_null) {
                            std::cout << "null,";
                        } else {
                            switch (type) {
                                case DatumType::Int8: {
                                    int64_t value = *((int64_t*)(msg.data() + off));
                                    off += sizeof(int64_t);
                                    std::cout << value << ",";
                                    break;
                                }
                                case DatumType::Float4: {
                                    float value = *((float*)(msg.data() + off));
                                    off += sizeof(float);
                                    std::cout << value << ",";
                                    break;
                                }
                                case DatumType::Text: {
                                    int len = *((int*)(msg.data() + off));
                                    off += sizeof(int);
                                    std::string s = msg.substr(off, len);
                                    off += len;
                                    std::cout << s << ",";
                                    break;
                                }
                                case DatumType::Bool: {
                                    bool value = *((bool*)(msg.data() + off));
                                    off += sizeof(bool);
                                    if (value) {
                                        std::cout << "true,";
                                    } else {
                                        std::cout << "false,";
                                    }
                                    break;
                                }
                                default:
                                    std::cout << "[Error],";
                                    break;
                            }
                        }
                    }
                    std::cout << std::endl;
                    break;
                }
                case 'C': {
                    //std::cout << msg << std::endl;
                    //command complete
                    //do any clean up after transaction is done
                    break;
                }
                case 'E':
                    //std::cout << msg << std::endl;
                    //error
                    //TODO: print out error message
                    //break out of inner loop to allow client to send new query
                    break;
                case 'Z':
                    //ready for query
                    //TODO: break out of inner loop to allow client to send new query
                    return true;
                default:
                    //invalid code
                    break;
            }
        }

        return true;
    }

private:
    int sockfd_;
};

}

int main(int argc, char** argv) {
    wsldb::Client client;

    client.Connect("127.0.0.1", "3000");


    if (argc > 1) {
        std::string line;
        std::string query;
        std::string path = argv[1];

        std::ifstream script(path);
        while (getline(script, line)) {
            query += line;
        }
        script.close();

        client.ProcessQuery(query);
    } else {
        while (true) {
            std::string query;
            getline(std::cin, query);
            if (!client.ProcessQuery(query))
                break;
        }
    }

}
