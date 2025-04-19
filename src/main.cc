#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <chrono>
#include <filesystem>
#include "localdb.h"

namespace fs = std::filesystem;

// Function to demonstrate multi-threaded read operations
void read_worker(localdb::Database* db, const std::string& table_name, int thread_id) {
    std::cout << "Read Thread " << thread_id << " started" << std::endl;
    
    for (int i = 0; i < 5; ++i) {
        // Begin a transaction
        auto transaction = db->beginTransaction();
        
        // Select all rows
        auto rows = transaction->select(table_name, [](const localdb::Row&) {
            return true; // Select all rows
        });
        
        std::cout << "Thread " << thread_id << " read " << rows.size() << " rows" << std::endl;
        
        // Commit the transaction
        transaction->commit();
        
        // Sleep for a short time
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    
    std::cout << "Read Thread " << thread_id << " finished" << std::endl;
}

// Function to demonstrate multi-threaded write operations
void write_worker(localdb::Database* db, const std::string& table_name, int thread_id) {
    std::cout << "Write Thread " << thread_id << " started" << std::endl;
    
    for (int i = 0; i < 5; ++i) {
        // Begin a transaction
        auto transaction = db->beginTransaction();
        
        // Create a new row
        localdb::Row row;
        row.push_back(localdb::Value(thread_id * 100 + i)); // ID
        row.push_back(localdb::Value(std::string("Data from thread ") + std::to_string(thread_id))); // Data
        
        // Insert the row
        bool success = transaction->insert(table_name, row);
        
        if (success) {
            std::cout << "Thread " << thread_id << " inserted row with ID " << (thread_id * 100 + i) << std::endl;
            
            // Commit the transaction
            transaction->commit();
        } else {
            std::cout << "Thread " << thread_id << " failed to insert row" << std::endl;
            
            // Rollback the transaction
            transaction->rollback();
        }
        
        // Sleep for a short time
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    std::cout << "Write Thread " << thread_id << " finished" << std::endl;
}

// Function to display all data in a table
void displayTable(localdb::Database& db, const std::string& table_name) {
    auto transaction = db.beginTransaction();
    auto rows = transaction->select(table_name, [](const localdb::Row&) {
        return true; // Select all rows
    });
    transaction->commit();
    
    std::cout << "\nTable contents for '" << table_name << "':" << std::endl;
    std::cout << "ID\tData" << std::endl;
    std::cout << "--\t----" << std::endl;
    
    for (const auto& row : rows) {
        std::cout << row[0].asInt() << "\t" << row[1].asText() << std::endl;
    }
}

// Function to demonstrate disk operations
void demo_disk_operations() {
    std::string db_filename = "localdb_data.bin";
    
    std::cout << "\n=== Disk Persistence Demo ===" << std::endl;
    
    // Create a database and populate it
    {
        std::cout << "Creating and populating database..." << std::endl;
        
        localdb::Database db;
        
        // Define columns for a table
        std::vector<localdb::Column> columns = {
            {"id", localdb::Column::INT, true, true, true},     // Primary key
            {"data", localdb::Column::TEXT, false, true, false} // Not null
        };
        
        // Create tables
        const std::string table1 = "users";
        const std::string table2 = "products";
        
        db.createTable(table1, columns);
        db.createTable(table2, columns);
        
        // Insert data into tables
        auto transaction = db.beginTransaction();
        
        // Insert users
        for (int i = 1; i <= 5; i++) {
            localdb::Row row;
            row.push_back(localdb::Value(i));
            row.push_back(localdb::Value(std::string("User ") + std::to_string(i)));
            
            transaction->insert(table1, row);
        }
        
        // Insert products
        for (int i = 101; i <= 105; i++) {
            localdb::Row row;
            row.push_back(localdb::Value(i));
            row.push_back(localdb::Value(std::string("Product ") + std::to_string(i)));
            
            transaction->insert(table2, row);
        }
        
        transaction->commit();
        
        // Display contents before saving
        std::cout << "\nDatabase tables before saving:" << std::endl;
        auto tables = db.getTableNames();
        for (const auto& table : tables) {
            std::cout << "- " << table << std::endl;
        }
        
        displayTable(db, table1);
        displayTable(db, table2);
        
        // Save database to file
        std::cout << "\nSaving database to file: " << db_filename << std::endl;
        bool save_result = db.saveToFile(db_filename);
        
        if (save_result) {
            std::cout << "Database saved successfully!" << std::endl;
            
            // Show file size
            try {
                auto filesize = fs::file_size(db_filename);
                std::cout << "File size: " << filesize << " bytes" << std::endl;
            } catch (const fs::filesystem_error& e) {
                std::cerr << "Error getting file size: " << e.what() << std::endl;
            }
        } else {
            std::cerr << "Failed to save database to file!" << std::endl;
        }
    }
    
    // Create a new database instance and load from file
    {
        std::cout << "\nCreating new database instance and loading from file..." << std::endl;
        
        localdb::Database db;
        
        // Load database from file
        bool load_result = db.loadFromFile(db_filename);
        
        if (load_result) {
            std::cout << "Database loaded successfully!" << std::endl;
            
            // Display contents after loading
            std::cout << "\nDatabase tables after loading:" << std::endl;
            auto tables = db.getTableNames();
            for (const auto& table : tables) {
                std::cout << "- " << table << std::endl;
            }
            
            for (const auto& table : tables) {
                displayTable(db, table);
            }
            
            // Make some changes to the loaded database
            std::cout << "\nMaking changes to the loaded database..." << std::endl;
            
            auto transaction = db.beginTransaction();
            
            // Add a new row
            localdb::Row new_row;
            new_row.push_back(localdb::Value(999));
            new_row.push_back(localdb::Value(std::string("New entry after loading")));
            
            transaction->insert("users", new_row);
            transaction->commit();
            
            // Display updated contents
            displayTable(db, "users");
            
            // Save the updated database
            std::cout << "\nSaving updated database to file..." << std::endl;
            db.saveToFile(db_filename);
            
            // Show file size after update
            try {
                auto filesize = fs::file_size(db_filename);
                std::cout << "Updated file size: " << filesize << " bytes" << std::endl;
            } catch (const fs::filesystem_error& e) {
                std::cerr << "Error getting file size: " << e.what() << std::endl;
            }
        } else {
            std::cerr << "Failed to load database from file!" << std::endl;
        }
    }
    
    // Clean up the file
    try {
        fs::remove(db_filename);
        std::cout << "\nCleaned up database file" << std::endl;
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error removing file: " << e.what() << std::endl;
    }
}

int main() {
    std::cout << "LocalDB Demo - Multi-threaded Relational Database" << std::endl;
    
    // 设置全局超时，防止程序运行超过10秒
    std::thread timeout_thread([]() {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        std::cerr << "\n程序运行超时（10秒），强制退出。可能存在死锁问题。" << std::endl;
        std::exit(1);
    });
    timeout_thread.detach();
    
    // Part 1: Demonstrate basic multi-threading capabilities
    {
        // Create a database
        localdb::Database db;
        
        // Define columns for a table
        std::vector<localdb::Column> columns = {
            {"id", localdb::Column::INT, true, true, true},  // Primary key
            {"data", localdb::Column::TEXT, false, true, false}
        };
        
        // Create a table
        const std::string table_name = "test_table";
        bool table_created = db.createTable(table_name, columns);
        
        if (!table_created) {
            std::cerr << "Failed to create table" << std::endl;
            return 1;
        }
        
        std::cout << "Table created successfully" << std::endl;
        
        // Insert some initial data
        auto transaction = db.beginTransaction();
        for (int i = 0; i < 5; ++i) {
            localdb::Row row;
            row.push_back(localdb::Value(i));  // ID
            row.push_back(localdb::Value(std::string("Initial data ") + std::to_string(i)));  // Data
            
            bool success = transaction->insert(table_name, row);
            if (!success) {
                std::cerr << "Failed to insert initial row " << i << std::endl;
            }
        }
        transaction->commit();
        
        // Create reader threads
        std::vector<std::thread> read_threads;
        for (int i = 0; i < 3; ++i) {
            read_threads.push_back(std::thread(read_worker, &db, table_name, i));
        }
        
        // Create writer threads
        std::vector<std::thread> write_threads;
        for (int i = 0; i < 2; ++i) {
            write_threads.push_back(std::thread(write_worker, &db, table_name, i));
        }
        
        // Wait for all threads to finish
        for (auto& thread : read_threads) {
            thread.join();
        }
        
        for (auto& thread : write_threads) {
            thread.join();
        }
        
        // Final query to show all data
        transaction = db.beginTransaction();
        auto rows = transaction->select(table_name, [](const localdb::Row&) {
            return true; // Select all rows
        });
        transaction->commit();
        
        std::cout << "\nFinal database contents:" << std::endl;
        std::cout << "ID\tData" << std::endl;
        std::cout << "--\t----" << std::endl;
        
        for (const auto& row : rows) {
            std::cout << row[0].asInt() << "\t" << row[1].asText() << std::endl;
        }
    }
    
    // Part 2: Demonstrate disk persistence
    demo_disk_operations();
    
    return 0;
}
