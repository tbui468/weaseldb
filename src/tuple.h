#pragma once

#include <iostream>
#include <algorithm>

#include "datum.h"
#include "schema.h"

namespace wsldb {

class Row {
public:
    virtual void SetCol(int col_idx, Datum d) = 0;
    virtual Datum GetCol(int col_idx) = 0;
    virtual std::vector<Datum> GetCols(int col_idx) = 0;
    virtual const std::vector<Datum>& Data() = 0;
};

class TupleGroup: public Row {
public:
    TupleGroup() {}
    void AddTuple(std::vector<Datum> tuple) {
        data_.push_back(std::move(tuple));
    }
    void SetCol(int col_idx, Datum d) {
        data_.at(0).at(col_idx) = d; //only sets col in first row
    }
    Datum GetCol(int col_idx) {
        return data_.at(0).at(col_idx); //only returns col in first row
    }
    std::vector<Datum> GetCols(int col_idx) {
        std::vector<Datum> result;
        for (std::vector<Datum> r: data_) {
            result.push_back(r.at(col_idx));
        }

        return result;
    }
    const std::vector<Datum>& Data() override {
        return data_.at(0); //only return first row
    }
private:
    std::vector<std::vector<Datum>> data_;
};

class Tuple: public Row {
public:
    Tuple(std::vector<Datum> data): data_(std::move(data)) {}
    void SetCol(int col_idx, Datum d) override {
        data_.at(col_idx) = d;
    }
    Datum GetCol(int col_idx) override {
        return data_.at(col_idx);
    }
    std::vector<Datum> GetCols(int col_idx) override {
        return {data_.at(col_idx)}; //just a single row if this is ever called
    }
    const std::vector<Datum>& Data() override {
        return data_;
    }
private:
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

        for (Row* t: tuples) {
            for (Datum d: t->Data()) {
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

public:
    std::vector<Row*> tuples;
};


}
