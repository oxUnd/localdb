#include "localdb.h"
#include <algorithm>
#include <stdexcept>
#include <iostream>
#include <fstream>

namespace localdb {

// Value implementation
Value::Value(const Value& other) : type(other.type) {
    switch (type) {
        case INT:
            int_val = other.int_val;
            break;
        case FLOAT:
            float_val = other.float_val;
            break;
        case TEXT:
            text_val = new std::string(*other.text_val);
            break;
        case BLOB:
            blob_val = new std::vector<uint8_t>(*other.blob_val);
            break;
        case NULL_TYPE:
            break;
    }
}

Value::Value(Value&& other) noexcept : type(other.type) {
    switch (type) {
        case INT:
            int_val = other.int_val;
            break;
        case FLOAT:
            float_val = other.float_val;
            break;
        case TEXT:
            text_val = other.text_val;
            other.text_val = nullptr;
            break;
        case BLOB:
            blob_val = other.blob_val;
            other.blob_val = nullptr;
            break;
        case NULL_TYPE:
            break;
    }
    other.type = NULL_TYPE;
}

Value& Value::operator=(const Value& other) {
    if (this != &other) {
        clear();
        type = other.type;
        switch (type) {
            case INT:
                int_val = other.int_val;
                break;
            case FLOAT:
                float_val = other.float_val;
                break;
            case TEXT:
                text_val = new std::string(*other.text_val);
                break;
            case BLOB:
                blob_val = new std::vector<uint8_t>(*other.blob_val);
                break;
            case NULL_TYPE:
                break;
        }
    }
    return *this;
}

Value& Value::operator=(Value&& other) noexcept {
    if (this != &other) {
        clear();
        type = other.type;
        switch (type) {
            case INT:
                int_val = other.int_val;
                break;
            case FLOAT:
                float_val = other.float_val;
                break;
            case TEXT:
                text_val = other.text_val;
                other.text_val = nullptr;
                break;
            case BLOB:
                blob_val = other.blob_val;
                other.blob_val = nullptr;
                break;
            case NULL_TYPE:
                break;
        }
        other.type = NULL_TYPE;
    }
    return *this;
}

Value::~Value() {
    clear();
}

void Value::clear() {
    switch (type) {
        case TEXT:
            delete text_val;
            break;
        case BLOB:
            delete blob_val;
            break;
        default:
            break;
    }
    type = NULL_TYPE;
}

int Value::asInt() const {
    if (type != INT) {
        throw std::runtime_error("Value is not an integer");
    }
    return int_val;
}

double Value::asFloat() const {
    if (type != FLOAT) {
        throw std::runtime_error("Value is not a float");
    }
    return float_val;
}

std::string Value::asText() const {
    if (type != TEXT) {
        throw std::runtime_error("Value is not a text");
    }
    return *text_val;
}

std::vector<uint8_t> Value::asBlob() const {
    if (type != BLOB) {
        throw std::runtime_error("Value is not a blob");
    }
    return *blob_val;
}

bool Value::operator==(const Value& other) const {
    if (type != other.type) {
        return false;
    }
    
    switch (type) {
        case INT:
            return int_val == other.int_val;
        case FLOAT:
            return float_val == other.float_val;
        case TEXT:
            return *text_val == *other.text_val;
        case BLOB:
            return *blob_val == *other.blob_val;
        case NULL_TYPE:
            return true;
    }
    
    return false;
}

bool Value::operator!=(const Value& other) const {
    return !(*this == other);
}

bool Value::operator<(const Value& other) const {
    if (type != other.type) {
        return type < other.type;
    }
    
    switch (type) {
        case INT:
            return int_val < other.int_val;
        case FLOAT:
            return float_val < other.float_val;
        case TEXT:
            return *text_val < *other.text_val;
        case BLOB:
            return *blob_val < *other.blob_val;
        case NULL_TYPE:
            return false;
    }
    
    return false;
}

// Value serialization
void Value::serialize(std::ostream& out) const {
    // Write the type
    out.write(reinterpret_cast<const char*>(&type), sizeof(type));
    
    // Write the data based on type
    switch (type) {
        case INT:
            out.write(reinterpret_cast<const char*>(&int_val), sizeof(int_val));
            break;
        case FLOAT:
            out.write(reinterpret_cast<const char*>(&float_val), sizeof(float_val));
            break;
        case TEXT: {
            size_t len = text_val->length();
            out.write(reinterpret_cast<const char*>(&len), sizeof(len));
            out.write(text_val->c_str(), len);
            break;
        }
        case BLOB: {
            size_t len = blob_val->size();
            out.write(reinterpret_cast<const char*>(&len), sizeof(len));
            if (len > 0) {
                out.write(reinterpret_cast<const char*>(blob_val->data()), len);
            }
            break;
        }
        case NULL_TYPE:
            // Nothing to write for null
            break;
    }
}

Value Value::deserialize(std::istream& in) {
    Value value;
    
    // Read the type
    in.read(reinterpret_cast<char*>(&value.type), sizeof(value.type));
    
    // Read the data based on type
    switch (value.type) {
        case INT:
            in.read(reinterpret_cast<char*>(&value.int_val), sizeof(value.int_val));
            break;
        case FLOAT:
            in.read(reinterpret_cast<char*>(&value.float_val), sizeof(value.float_val));
            break;
        case TEXT: {
            size_t len = 0;
            in.read(reinterpret_cast<char*>(&len), sizeof(len));
            
            std::string str(len, '\0');
            in.read(&str[0], len);
            
            value.text_val = new std::string(std::move(str));
            break;
        }
        case BLOB: {
            size_t len = 0;
            in.read(reinterpret_cast<char*>(&len), sizeof(len));
            
            std::vector<uint8_t> blob(len);
            if (len > 0) {
                in.read(reinterpret_cast<char*>(blob.data()), len);
            }
            
            value.blob_val = new std::vector<uint8_t>(std::move(blob));
            break;
        }
        case NULL_TYPE:
            // Nothing to read for null
            break;
    }
    
    return value;
}

// Table implementation
Table::Table(const std::string& name, const std::vector<Column>& columns)
    : name_(name), columns_(columns) {
    // Validate there's at most one primary key
    int primary_keys = 0;
    for (const auto& col : columns) {
        if (col.primary_key) {
            primary_keys++;
        }
    }
    
    if (primary_keys > 1) {
        throw std::runtime_error("Table can have at most one primary key");
    }
}

Table::~Table() = default;

bool Table::insert(const Row& row) {
    // Check if row size matches columns size
    if (row.size() != columns_.size()) {
        return false;
    }
    
    // Begin write lock
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // Check for primary key constraint
    int pk_index = findPrimaryKeyIndex();
    if (pk_index >= 0) {
        for (const auto& existing_row : rows_) {
            if (existing_row[pk_index] == row[pk_index]) {
                return false; // Primary key constraint violation
            }
        }
    }
    
    // Check for unique constraints
    for (size_t i = 0; i < columns_.size(); i++) {
        if (columns_[i].unique) {
            for (const auto& existing_row : rows_) {
                if (existing_row[i] == row[i]) {
                    return false; // Unique constraint violation
                }
            }
        }
    }
    
    // All constraints passed, insert the row
    rows_.push_back(row);
    return true;
}

bool Table::update(const Row& row, const std::function<bool(const Row&)>& predicate) {
    // Check if row size matches columns size
    if (row.size() != columns_.size()) {
        return false;
    }
    
    // Begin write lock
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    bool updated = false;
    for (auto& existing_row : rows_) {
        if (predicate(existing_row)) {
            existing_row = row;
            updated = true;
        }
    }
    
    return updated;
}

bool Table::remove(const std::function<bool(const Row&)>& predicate) {
    // Begin write lock
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    size_t original_size = rows_.size();
    rows_.erase(
        std::remove_if(rows_.begin(), rows_.end(), predicate),
        rows_.end()
    );
    
    return rows_.size() < original_size;
}

std::vector<Row> Table::select(const std::function<bool(const Row&)>& predicate) {
    // Begin read lock
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<Row> result;
    for (const auto& row : rows_) {
        if (predicate(row)) {
            result.push_back(row);
        }
    }
    
    return result;
}

const std::vector<Column>& Table::getColumns() const {
    return columns_;
}

const std::string& Table::getName() const {
    return name_;
}

bool Table::beginRead() {
    mutex_.lock_shared();
    return true;
}

void Table::endRead() {
    mutex_.unlock_shared();
}

bool Table::beginWrite() {
    mutex_.lock();
    return true;
}

void Table::endWrite() {
    mutex_.unlock();
}

int Table::findPrimaryKeyIndex() const {
    for (size_t i = 0; i < columns_.size(); i++) {
        if (columns_[i].primary_key) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

// Table serialization
void Table::serialize(std::ostream& out) const {
    // Making a copy of the data under a shared lock
    std::vector<Row> rows_copy;
    std::vector<Column> columns_copy;
    std::string name_copy;
    
    {
        std::shared_lock<std::shared_mutex> lock(const_cast<std::shared_mutex&>(mutex_));
        rows_copy = rows_;
        columns_copy = columns_;
        name_copy = name_;
    }
    
    // Write table name
    size_t name_len = name_copy.length();
    out.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
    out.write(name_copy.c_str(), name_len);
    
    // Write columns
    size_t col_count = columns_copy.size();
    out.write(reinterpret_cast<const char*>(&col_count), sizeof(col_count));
    
    for (const auto& col : columns_copy) {
        // Write column name
        size_t col_name_len = col.name.length();
        out.write(reinterpret_cast<const char*>(&col_name_len), sizeof(col_name_len));
        out.write(col.name.c_str(), col_name_len);
        
        // Write column type
        out.write(reinterpret_cast<const char*>(&col.type), sizeof(col.type));
        
        // Write constraints
        out.write(reinterpret_cast<const char*>(&col.primary_key), sizeof(col.primary_key));
        out.write(reinterpret_cast<const char*>(&col.not_null), sizeof(col.not_null));
        out.write(reinterpret_cast<const char*>(&col.unique), sizeof(col.unique));
    }
    
    // Write rows
    size_t row_count = rows_copy.size();
    out.write(reinterpret_cast<const char*>(&row_count), sizeof(row_count));
    
    for (const auto& row : rows_copy) {
        // Write values in the row
        size_t value_count = row.size();
        out.write(reinterpret_cast<const char*>(&value_count), sizeof(value_count));
        
        for (const auto& value : row) {
            value.serialize(out);
        }
    }
}

std::unique_ptr<Table> Table::deserialize(std::istream& in) {
    // Read table name
    size_t name_len = 0;
    in.read(reinterpret_cast<char*>(&name_len), sizeof(name_len));
    
    std::string table_name(name_len, '\0');
    in.read(&table_name[0], name_len);
    
    // Read columns
    size_t col_count = 0;
    in.read(reinterpret_cast<char*>(&col_count), sizeof(col_count));
    
    std::vector<Column> columns;
    columns.reserve(col_count);
    
    for (size_t i = 0; i < col_count; i++) {
        Column col;
        
        // Read column name
        size_t col_name_len = 0;
        in.read(reinterpret_cast<char*>(&col_name_len), sizeof(col_name_len));
        
        std::string col_name(col_name_len, '\0');
        in.read(&col_name[0], col_name_len);
        col.name = std::move(col_name);
        
        // Read column type
        in.read(reinterpret_cast<char*>(&col.type), sizeof(col.type));
        
        // Read constraints
        in.read(reinterpret_cast<char*>(&col.primary_key), sizeof(col.primary_key));
        in.read(reinterpret_cast<char*>(&col.not_null), sizeof(col.not_null));
        in.read(reinterpret_cast<char*>(&col.unique), sizeof(col.unique));
        
        columns.push_back(std::move(col));
    }
    
    // Create the table with name and columns
    auto table = std::make_unique<Table>(table_name, columns);
    
    // Read rows
    size_t row_count = 0;
    in.read(reinterpret_cast<char*>(&row_count), sizeof(row_count));
    
    for (size_t i = 0; i < row_count; i++) {
        // Read values in the row
        size_t value_count = 0;
        in.read(reinterpret_cast<char*>(&value_count), sizeof(value_count));
        
        Row row;
        row.reserve(value_count);
        
        for (size_t j = 0; j < value_count; j++) {
            row.push_back(Value::deserialize(in));
        }
        
        // Insert the row directly without triggering constraints check
        // since the data was already validated when it was first inserted
        table->rows_.push_back(std::move(row));
    }
    
    return table;
}

// Database implementation
Database::Database() = default;

Database::~Database() = default;

bool Database::createTable(const std::string& name, const std::vector<Column>& columns) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (tables_.find(name) != tables_.end()) {
        return false; // Table already exists
    }
    
    tables_[name] = std::make_unique<Table>(name, columns);
    return true;
}

bool Database::dropTable(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return false; // Table doesn't exist
    }
    
    tables_.erase(it);
    return true;
}

Table* Database::getTable(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return nullptr; // Table doesn't exist
    }
    
    return it->second.get();
}

std::shared_ptr<Transaction> Database::beginTransaction() {
    return std::make_shared<Transaction>(this);
}

// Transaction implementation
Transaction::Transaction(Database* db) : db_(db), active_(true) {}

Transaction::~Transaction() {
    if (active_) {
        rollback();
    }
}

bool Transaction::commit() {
    if (!active_) {
        return false;
    }
    
    active_ = false;
    rollback_operations_.clear();
    return true;
}

void Transaction::rollback() {
    if (!active_) {
        return;
    }
    
    active_ = false;
    
    // Apply rollback operations in reverse order
    for (auto it = rollback_operations_.rbegin(); it != rollback_operations_.rend(); ++it) {
        for (auto op_it = it->second.rbegin(); op_it != it->second.rend(); ++op_it) {
            (*op_it)();
        }
    }
    
    rollback_operations_.clear();
}

bool Transaction::insert(const std::string& table_name, const Row& row) {
    if (!active_) {
        return false;
    }
    
    Table* table = db_->getTable(table_name);
    if (!table) {
        return false;
    }
    
    if (table->beginWrite()) {
        bool result = table->insert(row);
        if (result) {
            // Add rollback operation
            rollback_operations_[table_name].push_back([table, row]() {
                table->remove([&row](const Row& r) {
                    return r == row;
                });
            });
        }
        table->endWrite();
        return result;
    }
    
    return false;
}

bool Transaction::update(const std::string& table_name, const Row& row, 
                       const std::function<bool(const Row&)>& predicate) {
    if (!active_) {
        return false;
    }
    
    Table* table = db_->getTable(table_name);
    if (!table) {
        return false;
    }
    
    if (table->beginWrite()) {
        // Get the rows that will be updated first for rollback
        std::vector<Row> original_rows = table->select(predicate);
        
        bool result = table->update(row, predicate);
        
        if (result && !original_rows.empty()) {
            // Add rollback operation for each updated row
            for (const auto& original_row : original_rows) {
                rollback_operations_[table_name].push_back([table, original_row]() {
                    table->update(original_row, [&original_row, table](const Row& r) {
                        // Use primary key to identify the row if available
                        int pk_index = table->findPrimaryKeyIndex();
                        if (pk_index >= 0) {
                            return r[pk_index] == original_row[pk_index];
                        }
                        // Otherwise use the whole row
                        return r == original_row;
                    });
                });
            }
        }
        
        table->endWrite();
        return result;
    }
    
    return false;
}

bool Transaction::remove(const std::string& table_name, 
                       const std::function<bool(const Row&)>& predicate) {
    if (!active_) {
        return false;
    }
    
    Table* table = db_->getTable(table_name);
    if (!table) {
        return false;
    }
    
    if (table->beginWrite()) {
        // Get the rows that will be deleted first for rollback
        std::vector<Row> deleted_rows = table->select(predicate);
        
        bool result = table->remove(predicate);
        
        if (result && !deleted_rows.empty()) {
            // Add rollback operation to reinsert all deleted rows
            rollback_operations_[table_name].push_back([table, deleted_rows]() {
                for (const auto& row : deleted_rows) {
                    table->insert(row);
                }
            });
        }
        
        table->endWrite();
        return result;
    }
    
    return false;
}

std::vector<Row> Transaction::select(const std::string& table_name,
                                   const std::function<bool(const Row&)>& predicate) {
    if (!active_) {
        return {};
    }
    
    Table* table = db_->getTable(table_name);
    if (!table) {
        return {};
    }
    
    if (table->beginRead()) {
        auto result = table->select(predicate);
        table->endRead();
        return result;
    }
    
    return {};
}

// Database serialization
bool Database::saveToFile(const std::string& filename) const {
    std::ofstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }
    
    // Make a copy of the tables map to avoid const issues
    std::unordered_map<std::string, std::unique_ptr<Table>> tables_copy;
    std::vector<std::string> table_names;
    
    {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
        
        // Get the table names
        table_names.reserve(tables_.size());
        for (const auto& [name, _] : tables_) {
            table_names.push_back(name);
        }
    }
    
    // Write number of tables
    size_t table_count = table_names.size();
    file.write(reinterpret_cast<const char*>(&table_count), sizeof(table_count));
    
    // Write each table
    for (const auto& name : table_names) {
        Table* table = nullptr;
        {
            std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
            auto it = tables_.find(name);
            if (it != tables_.end()) {
                table = it->second.get();
            }
        }
        
        if (table) {
            table->serialize(file);
        }
    }
    
    return file.good();
}

bool Database::loadFromFile(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Clear existing tables
    tables_.clear();
    
    // Read number of tables
    size_t table_count = 0;
    file.read(reinterpret_cast<char*>(&table_count), sizeof(table_count));
    
    // Read each table
    for (size_t i = 0; i < table_count; i++) {
        auto table = Table::deserialize(file);
        if (table) {
            tables_[table->getName()] = std::move(table);
        } else {
            return false;
        }
    }
    
    return file.good();
}

std::vector<std::string> Database::getTableNames() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
    
    std::vector<std::string> names;
    names.reserve(tables_.size());
    
    for (const auto& [name, _] : tables_) {
        names.push_back(name);
    }
    
    return names;
}

} // namespace localdb
