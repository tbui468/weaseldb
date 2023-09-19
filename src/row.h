#pragma once

#include <iostream>
#include <algorithm>
#include <unordered_map>

#include "datum.h"

namespace wsldb {

//Row will have either a single tuple (just the input row) or many (group row)
class Row {
public:
    Row(std::vector<Datum> data): data_(data) {}
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
                if (d.Type() == TokenType::Int4) {
                    std::cout << d.AsInt4() << ",";
                } else if (d.Type() == TokenType::Int8) {
                    std::cout << d.AsInt8() << ",";
                } else if (d.Type() == TokenType::Float4) {
                    std::cout << d.AsFloat4() << ",";
                } else if (d.Type() == TokenType::Text) {
                    std::cout << d.AsString() << ",";
                } else if (d.Type() == TokenType::Null) {
                    std::cout << "null,";
                } else if (d.Type() == TokenType::Bool) {
                    if (d.AsBool()) {
                        std::cout << "true,";
                    } else {
                        std::cout << "false,";
                    }
                } else {
                    std::cout << "unsupported data type,";
                }
            }
            std::cout << std::endl;
        }
    }

public:
    std::vector<Row*> rows_;
};


}
