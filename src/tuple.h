#pragma once

#include <iostream>

#include "datum.h"
#include "schema.h"

namespace wsldb {

class Tuple {
public:
    Tuple(std::vector<Datum> data): data(std::move(data)) {}
    std::vector<Datum> data;
};

class TupleSet {
public:
    TupleSet(std::vector<std::string> fields): fields(std::move(fields)) {}
    std::vector<std::string> fields;
    std::vector<Tuple*> tuples;
    void Print() {
        for (const std::string& f: fields) {
            std::cout << f << ", ";
        }
        std::cout << std::endl;

        for (Tuple* t: tuples) {
            for (Datum d: t->data) {
                if (d.Type() == TokenType::Int) {
                    std::cout << d.AsInt() << ", ";
                } else if (d.Type() == TokenType::Text) {
                    std::cout << d.AsString() << ", ";
                } else {
                    if (d.AsBool()) {
                        std::cout << "true, ";
                    } else {
                        std::cout << "false, ";
                    }
                }
            }
            std::cout << std::endl;
        }
    }
};


}
