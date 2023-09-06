#pragma once

#include <iostream>
#include <algorithm>

#include "datum.h"
#include "schema.h"

namespace wsldb {

//Row will have either a single tuple (just the input row) or many (group row)
class Row {
public:
    Row(std::vector<Datum> data): data_({data}) {}
public:
    std::vector<Datum> data_;
};

class RowSet {
public:
    void Print() {
        /*
        for (const std::string& f: fields) {
            std::cout << f << ", ";
        }
        std::cout << std::endl;*/

        for (Row* r: rows_) {
            for (Datum d: r->data_) {
                if (d.Type() == TokenType::Int) {
                    std::cout << d.AsInt() << ",";
                } else if (d.Type() == TokenType::Float4) {
                    std::cout << d.AsFloat4() << ",";
                } else if (d.Type() == TokenType::Text) {
                    std::cout << d.AsString() << ",";
                } else {
                    if (d.AsBool()) {
                        std::cout << "true,";
                    } else {
                        std::cout << "false,";
                    }
                }
            }
            std::cout << std::endl;
        }
    }

public:
    std::vector<Row*> rows_;
};


}
