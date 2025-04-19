#ifndef LOCALDB_H
#define LOCALDB_H

#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <functional>
#include <atomic>
#include <condition_variable>
#include <shared_mutex>
#include <fstream>
#include <chrono>
#include <thread>

// Fallback for shared_mutex if not available in the standard library
#if __cplusplus < 201703L
namespace std {
    class shared_mutex {
    private:
        std::mutex mutex_;
        std::atomic<int> shared_count_ = {0};
        std::condition_variable cv_;
        bool exclusive_ = false;
    public:
        void lock() {
            std::unique_lock<std::mutex> lk(mutex_);
            cv_.wait(lk, [this] { return shared_count_ == 0 && !exclusive_; });
            exclusive_ = true;
        }
        
        void unlock() {
            std::unique_lock<std::mutex> lk(mutex_);
            exclusive_ = false;
            cv_.notify_all();
        }
        
        void lock_shared() {
            std::unique_lock<std::mutex> lk(mutex_);
            cv_.wait(lk, [this] { return !exclusive_; });
            ++shared_count_;
        }
        
        void unlock_shared() {
            std::unique_lock<std::mutex> lk(mutex_);
            --shared_count_;
            if (shared_count_ == 0) {
                cv_.notify_all();
            }
        }
        
        bool try_lock() {
            std::unique_lock<std::mutex> lk(mutex_, std::try_to_lock);
            if (!lk.owns_lock() || shared_count_ > 0 || exclusive_) {
                return false;
            }
            exclusive_ = true;
            return true;
        }
        
        bool try_lock_shared() {
            std::unique_lock<std::mutex> lk(mutex_, std::try_to_lock);
            if (!lk.owns_lock() || exclusive_) {
                return false;
            }
            ++shared_count_;
            return true;
        }
        
        template<class Rep, class Period>
        bool try_lock_for(const std::chrono::duration<Rep, Period>& duration) {
            std::unique_lock<std::mutex> lk(mutex_);
            if (!cv_.wait_for(lk, duration, [this] { return shared_count_ == 0 && !exclusive_; })) {
                return false;
            }
            exclusive_ = true;
            return true;
        }
        
        template<class Rep, class Period>
        bool try_lock_shared_for(const std::chrono::duration<Rep, Period>& duration) {
            std::unique_lock<std::mutex> lk(mutex_);
            if (!cv_.wait_for(lk, duration, [this] { return !exclusive_; })) {
                return false;
            }
            ++shared_count_;
            return true;
        }
    };
    
    template<class Mutex>
    class shared_lock {
    private:
        Mutex* mutex_;
        bool owns_;
    public:
        explicit shared_lock(Mutex& m) : mutex_(&m), owns_(true) { mutex_->lock_shared(); }
        ~shared_lock() { if (owns_) mutex_->unlock_shared(); }
    };
    
    template<class Mutex>
    class unique_lock {
    private:
        Mutex* mutex_;
        bool owns_;
    public:
        explicit unique_lock(Mutex& m) : mutex_(&m), owns_(true) { mutex_->lock(); }
        ~unique_lock() { if (owns_) mutex_->unlock(); }
    };
}
#endif

namespace localdb {

// Forward declarations
class Table;
class Database;
class Transaction;

// Column definition
struct Column {
    std::string name;
    enum Type {
        INT,
        FLOAT,
        TEXT,
        BLOB
    } type;
    bool primary_key = false;
    bool not_null = false;
    bool unique = false;
};

// Value type that can store any supported data type
class Value {
public:
    enum Type {
        NULL_TYPE,
        INT,
        FLOAT,
        TEXT,
        BLOB
    } type;

    // Constructors
    Value() : type(NULL_TYPE) {}
    explicit Value(int val) : type(INT), int_val(val) {}
    explicit Value(double val) : type(FLOAT), float_val(val) {}
    explicit Value(const std::string& val) : type(TEXT), text_val(new std::string(val)) {}
    explicit Value(const std::vector<uint8_t>& val) : type(BLOB), blob_val(new std::vector<uint8_t>(val)) {}

    // Copy and move constructors
    Value(const Value& other);
    Value(Value&& other) noexcept;

    // Assignment operators
    Value& operator=(const Value& other);
    Value& operator=(Value&& other) noexcept;

    // Destructor
    ~Value();

    // Getters
    int asInt() const;
    double asFloat() const;
    std::string asText() const;
    std::vector<uint8_t> asBlob() const;

    // Comparison operators
    bool operator==(const Value& other) const;
    bool operator!=(const Value& other) const;
    bool operator<(const Value& other) const;

    // Serialization/Deserialization
    void serialize(std::ostream& out) const;
    static Value deserialize(std::istream& in);

private:
    union {
        int int_val;
        double float_val;
        std::string* text_val;
        std::vector<uint8_t>* blob_val;
    };

    void clear();
};

// Row representation
using Row = std::vector<Value>;

// Table class
class Table {
public:
    Table(const std::string& name, const std::vector<Column>& columns);
    ~Table();

    // Basic operations
    bool insert(const Row& row);
    bool update(const Row& row, const std::function<bool(const Row&)>& predicate);
    bool remove(const std::function<bool(const Row&)>& predicate);
    
    // Query operations
    std::vector<Row> select(const std::function<bool(const Row&)>& predicate);
    
    // Schema operations
    const std::vector<Column>& getColumns() const;
    const std::string& getName() const;

    // Thread-safe operations
    bool beginRead(std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));
    void endRead();
    bool beginWrite(std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));
    void endWrite();

    // Serialization/Deserialization
    void serialize(std::ostream& out) const;
    static std::unique_ptr<Table> deserialize(std::istream& in);

    // Thread synchronization (public for low-level access by Transaction)
    std::shared_mutex mutex_;

private:
    std::string name_;
    std::vector<Column> columns_;
    std::vector<Row> rows_;
    
    // Find primary key column index
    int findPrimaryKeyIndex() const;
    
    friend class Transaction;
    friend class Database;
};

// Database class
class Database {
public:
    Database();
    ~Database();

    // Table operations
    bool createTable(const std::string& name, const std::vector<Column>& columns);
    bool dropTable(const std::string& name);
    Table* getTable(const std::string& name);
    
    // Transaction support
    std::shared_ptr<Transaction> beginTransaction();
    
    // Disk persistence
    bool saveToFile(const std::string& filename) const;
    bool loadFromFile(const std::string& filename);
    
    // Get all table names
    std::vector<std::string> getTableNames() const;
    
private:
    std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
    std::mutex mutex_;
};

// Transaction class for ACID properties
class Transaction {
public:
    explicit Transaction(Database* db);
    ~Transaction();
    
    // Transaction operations
    bool commit();
    void rollback();
    
    // Table operations within transaction
    bool insert(const std::string& table_name, const Row& row);
    bool update(const std::string& table_name, const Row& row, 
                const std::function<bool(const Row&)>& predicate);
    bool remove(const std::string& table_name, 
                const std::function<bool(const Row&)>& predicate);
    std::vector<Row> select(const std::string& table_name,
                            const std::function<bool(const Row&)>& predicate);
    
private:
    Database* db_;
    bool active_;
    std::map<std::string, std::vector<std::function<void()>>> rollback_operations_;
};

// Example retry logic for transaction operations
inline bool performTransactionOperation(std::function<bool()> operation, int maxRetries = 3) {
    for (int attempt = 0; attempt < maxRetries; attempt++) {
        if (operation()) {
            return true;
        }
        // Exponential backoff
        std::this_thread::sleep_for(std::chrono::milliseconds(10 * (1 << attempt)));
    }
    return false;
}

} // namespace localdb

#endif // LOCALDB_H
