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

Table::Table(std::string table_name,
             std::vector<Token> names,
             std::vector<Token> types,
             std::vector<bool> not_null_constraints, 
             std::vector<std::vector<Token>> uniques) {

    table_name_ = table_name;
    rowid_counter_ = 0;

    //NOTE: index creation requires attrs_ to be filled in beforehand
    for (size_t i = 0; i < names.size(); i++) {
        attrs_.emplace_back(names.at(i).lexeme, types.at(i).type, not_null_constraints.at(i));
    }

    //indexes
    for (const std::vector<Token>& col_group: uniques) {
        std::vector<int> idx_cols;
        for (Token t: col_group) {
            idx_cols.push_back(GetAttrIdx(t.lexeme));
        }

        idxs_.emplace_back(IdxName(idx_cols), idx_cols);
    }
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

    //deserialize indexes
    int idx_count = *((int*)(buf.data() + off));
    off += sizeof(int);
    for (int i = 0; i < idx_count; i++) {
        idxs_.emplace_back(buf, &off);
    }
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

    //serialize indexes
    int idx_count = idxs_.size();
    buf.append((char*)&idx_count, sizeof(int));
    for (Index& i: idxs_) {
        buf += i.Serialize();
    }

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

int Table::GetAttrIdx(const std::string& name) {
    for (size_t i = 0; i < attrs_.size(); i++) {
        if (name.compare(attrs_.at(i).name) == 0) {
            return i;
        }
    }

    return -1;
}

Status Table::BeginScan(Storage* storage) {
    int cf_idx = 1;

    TableHandle handle = storage->GetTableHandle(table_name_);
    it_ = handle.db->NewIterator(rocksdb::ReadOptions(), handle.cfs.at(cf_idx));

    it_->SeekToFirst();

    return Status(true, "ok");
}

Status Table::NextRow(Storage* storage, Row** r) {
    if (!it_->Valid()) return Status(false, "no more record");

    std::string value = it_->value().ToString();
    *r = new Row(DeserializeData(value));
    it_->Next();

    return Status(true, "ok");
}

Status Table::DeletePrev(Storage* storage, Batch* batch) {
    int cf_idx = 1;

    it_->Prev();
    std::string key = it_->key().ToString();

    TableHandle handle = storage->GetTableHandle(table_name_);
    batch->Delete(table_name_, handle.cfs.at(cf_idx), key);

    it_->Next();
    return Status(true, "ok");
}

Status Table::UpdatePrev(Storage* storage, Batch* batch, Row* r) {
    int cf_idx = 1;

    it_->Prev();
    std::string old_key = it_->key().ToString();
    std::string new_key = Idx(0).GetKeyFromFields(r->data_);

    TableHandle handle = storage->GetTableHandle(table_name_);
    if (old_key.compare(new_key) != 0) {
        batch->Delete(table_name_, handle.cfs.at(cf_idx), old_key);
    }
    batch->Put(table_name_, handle.cfs.at(cf_idx), new_key, Datum::SerializeData(r->data_));
    it_->Next();

    return Status(true, "ok");
}

Status Table::Insert(Storage* storage, Batch* batch, std::vector<Datum>& data) {
    int cf_idx = 1;
    //insert _rowid
    int64_t rowid = NextRowId();
    data.at(0) = Datum(rowid);

    std::string value = Datum::SerializeData(data);
    std::string key = Idx(0).GetKeyFromFields(data);

    std::string test_value;
    TableHandle handle = storage->GetTableHandle(table_name_);
    rocksdb::Status status = handle.db->Get(rocksdb::ReadOptions(), handle.cfs.at(cf_idx), key, &test_value);

    if (status.ok()) {
        return Status(false, "Error: A record with the same primary key already exists");
    }

    batch->Put(table_name_, handle.cfs.at(cf_idx), key, value);

    //Writing table back to disk to ensure autoincrementing rowid is updated
    //TODO: optimzation opportunity - only need to write table once all inserts are done, not after each one
    int default_name_idx = 0;
    TableHandle catalogue = storage->GetTableHandle(storage->CatalogueTableName());
    batch->Put(storage->CatalogueTableName(), catalogue.cfs.at(default_name_idx), table_name_, Serialize());

    return Status(true, "ok");
}

}
