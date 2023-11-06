#include <algorithm>

#include "table.h"

namespace wsldb {

Schema::Schema(std::string table_name,
               std::vector<Token> names,
               std::vector<Token> types,
               std::vector<bool> not_null_constraints, 
               std::vector<std::vector<Token>> uniques) {

    table_name_ = table_name;
    rowid_counter_ = 0;

    //NOTE: index creation requires attrs_ to be filled in beforehand
    for (size_t i = 0; i < names.size(); i++) {
        attrs_.emplace_back(names.at(i).lexeme, TypeTokenToDatumType(types.at(i).type), not_null_constraints.at(i));
    }

    bool is_primary = true;

    //indexes
    for (const std::vector<Token>& col_group: uniques) {
        std::vector<int> idx_cols;
        for (Token t: col_group) {
            idx_cols.push_back(GetAttrIdx(t.lexeme));
        }

        if (is_primary) {
            idxs_.emplace_back(IdxName("primary", idx_cols), idx_cols);
            is_primary = false;
            continue;
        }

        idxs_.emplace_back(IdxName("secondary", idx_cols), idx_cols);
    }
}

Schema::Schema(std::string table_name, const std::string& buf) {
    table_name_ = table_name;

    int off = 0; 

    rowid_counter_ = *((int64_t*)(buf.data() + off));
    off += sizeof(int64_t);

    //attributes
    int count = *((int*)(buf.data() + off));
    off += sizeof(int);

    for (int i = 0; i < count; i++) {
        DatumType type = *((DatumType*)(buf.data() + off));
        off += sizeof(DatumType);

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

std::string Schema::Serialize() const {
    std::string buf;

    buf.append((char*)&rowid_counter_, sizeof(int64_t));

    //attributes
    int count = attrs_.size();
    buf.append((char*)&count, sizeof(count));

    for (const Attribute& a: attrs_) {
        buf.append((char*)&a.type, sizeof(DatumType));
        int str_size = a.name.length();
        buf.append((char*)&str_size, sizeof(int));
        buf += a.name;
        buf.append((char*)&a.not_null_constraint, sizeof(bool));
    }

    //serialize indexes
    int idx_count = idxs_.size();
    buf.append((char*)&idx_count, sizeof(int));
    for (const Index& i: idxs_) {
        buf += i.Serialize();
    }

    return buf;
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
        attrs_.emplace_back(names.at(i).lexeme, TypeTokenToDatumType(types.at(i).type), not_null_constraints.at(i));
    }

    bool is_primary = true;

    //indexes
    for (const std::vector<Token>& col_group: uniques) {
        std::vector<int> idx_cols;
        for (Token t: col_group) {
            idx_cols.push_back(GetAttrIdx(t.lexeme));
        }

        if (is_primary) {
            idxs_.emplace_back(IdxName("primary", idx_cols), idx_cols);
            is_primary = false;
            continue;
        }

        idxs_.emplace_back(IdxName("secondary", idx_cols), idx_cols);
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
        DatumType type = *((DatumType*)(buf.data() + off));
        off += sizeof(DatumType);

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
        buf.append((char*)&a.type, sizeof(DatumType));
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

Status Table::BeginScan(Storage* storage, int scan_idx) {
    scan_idx_ = scan_idx;

    TableHandle handle = storage->GetTable(table_name_);
    it_ = handle.db->NewIterator(rocksdb::ReadOptions(), handle.cfs.at(scan_idx_));

    it_->SeekToFirst();

    return Status(true, "ok");
}

Status Table::NextRow(Storage* storage, Row** r) {
    if (!it_->Valid()) return Status(false, "no more records");

    std::string value = it_->value().ToString();
  
    //if scan is using secondary index, value is primary key
    //return record stored in primary index
    int primary_cf = 0; 
    if (scan_idx_ != primary_cf) {
        TableHandle handle = storage->GetTable(table_name_);
        std::string primary_key = value;
        handle.db->Get(rocksdb::ReadOptions(), handle.cfs.at(primary_cf), primary_key, &value);
    }

    *r = new Row(DeserializeData(value));
    it_->Next();

    return Status(true, "ok");
}


//TODO: make sure to complete copy constructor for Row class
Status Table::UpdateRow(Storage* storage, Batch* batch, Row* updated_row, Row* old_row) {
    int primary_cf = 0; 

    TableHandle handle = storage->GetTable(table_name_);
    std::string old_key = Idx(primary_cf).GetKeyFromFields(old_row->data_);
    std::string updated_primary_key = Idx(primary_cf).GetKeyFromFields(updated_row->data_);

    if (old_key.compare(updated_primary_key) != 0) {
        std::string tmp_value;
        if (handle.db->Get(rocksdb::ReadOptions(), handle.cfs.at(primary_cf), updated_primary_key, &tmp_value).ok())
            return Status(false, "Error: A record with the same primary key already exists");

        batch->Delete(table_name_, handle.cfs.at(primary_cf), old_key);
    }

    batch->Put(table_name_, handle.cfs.at(primary_cf), updated_primary_key, Datum::SerializeData(updated_row->data_));

    for (size_t i = 1; i < idxs_.size(); i++) {
        std::string old_key = Idx(i).GetKeyFromFields(old_row->data_);
        std::string updated_key = Idx(i).GetKeyFromFields(updated_row->data_);

        if (old_key.compare(updated_key) != 0) {
            std::string tmp_value;
            if (handle.db->Get(rocksdb::ReadOptions(), handle.cfs.at(i), updated_key, &tmp_value).ok())
                return Status(false, "Error: A record with the same secondary key already exists");

            batch->Delete(table_name_, handle.cfs.at(i), old_key);
        }

        batch->Put(table_name_, handle.cfs.at(i), updated_key, updated_primary_key);
    }

    return Status(true, "ok");
}


}
