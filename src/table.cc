#include <algorithm>

#include "table.h"

namespace wsldb {

Index::Index(const std::string& buf, int* offset) {
    name_ = Datum(buf, offset, TokenType::Text).AsString();

    int count = Datum(buf, offset, TokenType::Int4).AsInt4();
    for (int i = 0; i < count; i++) {
        int idx = Datum(buf, offset, TokenType::Int4).AsInt4();
        key_idxs_.push_back(idx);
    }
}

std::string Index::Serialize() const {
    std::string result = Datum(name_).Serialize();

    int col_count = key_idxs_.size();
    result += Datum(col_count).Serialize();

    for (int i: key_idxs_) {
        result += Datum(i).Serialize();
    }

    return result;
}

Table::Table(std::string table_name, std::vector<Token> names, std::vector<Token> types, std::vector<bool> not_null_constraints, std::vector<Token> primary_keys) {
    table_name_ = table_name;

    for (size_t i = 0; i < names.size(); i++) {
        attrs_.emplace_back(names.at(i).lexeme, types.at(i).type, not_null_constraints.at(i));
    }

    std::vector<int> primary_idx_cols;
    for (Token t: primary_keys) {
        primary_idx_cols.push_back(GetAttrIdx(t.lexeme));
    }

    primary_idx_ = Index(Table::PrimaryIdxName(table_name), primary_idx_cols);

    //TODO: generate secondary idxs here

    rowid_counter_ = 0;
}

Table::Table(std::string table_name, const std::string& buf) {
    table_name_ = table_name;

    int off = 0; 

    rowid_counter_ = *((int64_t*)(buf.data() + off));
    off += sizeof(int64_t);

    //attributes
    int count = *((int*)(buf.data() + off));
    off += sizeof(int);

    for (int i = 0; i < count; i++) {
        TokenType type = *((TokenType*)(buf.data() + off));
        off += sizeof(TokenType);

        int str_size = *((int*)(buf.data() + off));
        off += sizeof(int);
        std::string name = buf.substr(off, str_size);
        off += str_size;

        bool not_null_constraint = *((bool*)(buf.data() + off));
        off += sizeof(bool);

        attrs_.emplace_back(name, type, not_null_constraint);
    }

    //primary idx
    primary_idx_ = Index(buf, &off);

    //secondary idxs

}

std::string Table::Serialize() {
    std::string buf;

    buf.append((char*)&rowid_counter_, sizeof(int64_t));

    //attributes
    int count = attrs_.size();
    buf.append((char*)&count, sizeof(count));

    for (const Attribute& a: attrs_) {
        buf.append((char*)&a.type, sizeof(TokenType));
        int str_size = a.name.length();
        buf.append((char*)&str_size, sizeof(int));
        buf += a.name;
        buf.append((char*)&a.not_null_constraint, sizeof(bool));
    }

    //primary idx
    buf += primary_idx_.Serialize();

    //secondary idxs
    //count, followed by serialized index

    return buf;
}

std::vector<Datum> Table::DeserializeData(const std::string& value) {
    std::vector<Datum> data = std::vector<Datum>();
    int off = 0;
    for (const Attribute& a: attrs_) {
        data.push_back(Datum(value, &off, a.type));
    }

    return data;
}

//TODO: Generalize to work with primary and secondary idxs
std::string Table::GetKeyFromData(const std::vector<Datum>& data) {
    std::string primary_key;
    for (int i: primary_idx_.key_idxs_) {
        primary_key += data.at(i).Serialize();
    }
    return primary_key;
}

int Table::GetAttrIdx(const std::string& name) {
    for (size_t i = 0; i < attrs_.size(); i++) {
        if (name.compare(attrs_.at(i).name) == 0) {
            return i;
        }
    }

    return -1;
}

Status Table::BeginScan(DB* db) {
    it_ = db->GetIdxHandle(table_name_)->NewIterator(rocksdb::ReadOptions());
    it_->SeekToFirst();

    return Status(true, "ok");
}
Status Table::NextRow(DB* db, Row** r) {
    if (!it_->Valid()) return Status(false, "no more record");

    std::string value = it_->value().ToString();
    *r = new Row(DeserializeData(value));
    it_->Next();

    return Status(true, "ok");
}

Status Table::DeletePrev(DB* db) {
    it_->Prev();
    std::string key = it_->key().ToString();
    db->GetIdxHandle(table_name_)->Delete(rocksdb::WriteOptions(), key);
    it_->Next();
    return Status(true, "ok");
}
Status Table::UpdatePrev(DB* db, Row* r) {
    it_->Prev();
    std::string old_key = it_->key().ToString();
    std::string new_key = GetKeyFromData(r->data_);

    rocksdb::DB* tab_handle = db->GetIdxHandle(table_name_);
    if (old_key.compare(new_key) != 0) {
        tab_handle->Delete(rocksdb::WriteOptions(), old_key);
    }
    tab_handle->Put(rocksdb::WriteOptions(), new_key, Datum::SerializeData(r->data_));
    it_->Next();
    return Status(true, "ok");
}
Status Table::Insert(DB* db, std::vector<Datum>& data) {
    rocksdb::DB* tab_handle = db->GetIdxHandle(table_name_);

    //insert _rowid
    int64_t rowid = NextRowId();
    data.at(0) = Datum(rowid);

    std::string value = Datum::SerializeData(data);
    std::string key = GetKeyFromData(data);

    std::string test_value;
    rocksdb::Status status = tab_handle->Get(rocksdb::ReadOptions(), key, &test_value);
    if (status.ok()) {
        return Status(false, "Error: A record with the same primary key already exists");
    }

    tab_handle->Put(rocksdb::WriteOptions(), key, value);

    //Writing table back to disk to ensure autoincrementing rowid is updated
    //TODO: optimzation opportunity - only need to write table once all inserts are done, not after each one
    db->Catalogue()->Put(rocksdb::WriteOptions(), table_name_, Serialize());
    return Status(true, "ok");
}

}
