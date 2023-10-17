#pragma once

#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <vector>

#include "datum.h"

namespace wsldb {

struct Attribute {
    Attribute(const std::string& name, DatumType type, bool not_null_constraint):
        name(name), type(type), not_null_constraint(not_null_constraint) {}

    std::string name;
    DatumType type;
    bool not_null_constraint;

    std::string ToString() const {
        std::string ret;

        ret += name + ",";
        ret += Datum::TypeToString(type) + ",";
        ret += not_null_constraint ? "true" : "false";

        return ret;
    }
};

//Row will have either a single tuple (just the input row) or many (group row)
class Row {
public:
    Row(std::vector<Datum> data): data_(data) {}
    std::string Serialize() const {
        std::string ret;

        for (const Datum& d: data_) {
            ret += d.Serialize();
        }

        return ret;
    }
    void Print() {
        for (Datum d: data_) {
            if (d.IsType(DatumType::Int4)) {
                std::cout << d.AsInt4() << ",";
            } else if (d.IsType(DatumType::Int8)) {
                std::cout << d.AsInt8() << ",";
            } else if (d.IsType(DatumType::Float4)) {
                std::cout << d.AsFloat4() << ",";
            } else if (d.IsType(DatumType::Text)) {
                std::cout << d.AsString() << ",";
            } else if (d.IsType(DatumType::Null)) {
                std::cout << "null,";
            } else if (d.IsType(DatumType::Bool)) {
                if (d.AsBool()) {
                    std::cout << "true,";
                } else {
                    std::cout << "false,";
                }
            } else {
                std::cout << "unsupported data type,";
            }
        }
    }
public:
    std::vector<Datum> data_;
};

class RowSet {
public:
    RowSet(std::vector<Attribute> attrs): attrs_(attrs) {}
    void Print() {
        for (Row* r: rows_) {
            r->Print();
            std::cout << std::endl;
        }
    }
    std::string SerializeRowDescription() {
        std::string ret;

        int count = attrs_.size();
        ret.append((char*)&count, sizeof(int));

        for (const Attribute& a: attrs_) {
            ret.append((char*)&a.type, sizeof(DatumType));
            int s_size = a.name.size();
            ret.append((char*)&s_size, sizeof(int));
            ret += a.name;
        }

        return ret;
    }
    std::vector<std::string> SerializeDataRows() {
        std::vector<std::string> ret;

        for (Row* r: rows_) {
            ret.push_back(r->Serialize());
        }

        return ret;
    }
public:
    std::vector<Attribute> attrs_;
    std::vector<Row*> rows_;
};


}
