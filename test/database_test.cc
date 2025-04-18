#include <gtest/gtest.h>
#include "localdb.h"
#include <string>
#include <vector>
#include <stdexcept>
#include <fstream>

namespace {

class DatabaseTest : public ::testing::Test {
protected:
    std::vector<localdb::Column> user_columns;
    std::vector<localdb::Column> product_columns;
    
    void SetUp() override {
        user_columns = {
            {"id", localdb::Column::INT, true, true, true},     // Primary key
            {"name", localdb::Column::TEXT, false, true, false}, // Not null
            {"age", localdb::Column::INT, false, false, false}   // Regular column
        };
        
        product_columns = {
            {"product_id", localdb::Column::INT, true, true, true},   // Primary key
            {"name", localdb::Column::TEXT, false, true, false},       // Not null
            {"price", localdb::Column::FLOAT, false, false, false}     // Regular column
        };
    }

    void TearDown() override {
        // Any cleanup
    }
    
    localdb::Row createUserRow(int id, const std::string& name, int age) {
        localdb::Row row;
        row.push_back(localdb::Value(id));
        row.push_back(localdb::Value(name));
        row.push_back(localdb::Value(age));
        return row;
    }
    
    localdb::Row createProductRow(int product_id, const std::string& name, double price) {
        localdb::Row row;
        row.push_back(localdb::Value(product_id));
        row.push_back(localdb::Value(name));
        row.push_back(localdb::Value(price));
        return row;
    }
};

// Test Database Creation and Table Management
TEST_F(DatabaseTest, DatabaseCreation) {
    localdb::Database db;
    
    // Create a table
    EXPECT_TRUE(db.createTable("users", user_columns));
    
    // Create a different table
    EXPECT_TRUE(db.createTable("products", product_columns));
    
    // Try to create a duplicate table
    EXPECT_FALSE(db.createTable("users", user_columns));
    
    // Get tables
    localdb::Table* users_table = db.getTable("users");
    ASSERT_NE(users_table, nullptr);
    EXPECT_EQ(users_table->getName(), "users");
    
    localdb::Table* products_table = db.getTable("products");
    ASSERT_NE(products_table, nullptr);
    EXPECT_EQ(products_table->getName(), "products");
    
    // Get non-existent table
    EXPECT_EQ(db.getTable("non_existent"), nullptr);
    
    // Drop table
    EXPECT_TRUE(db.dropTable("users"));
    
    // Try to get dropped table
    EXPECT_EQ(db.getTable("users"), nullptr);
    
    // Try to drop non-existent table
    EXPECT_FALSE(db.dropTable("non_existent"));
}

// Test Database Table Operations
TEST_F(DatabaseTest, DatabaseTableOperations) {
    localdb::Database db;
    
    // Create tables
    EXPECT_TRUE(db.createTable("users", user_columns));
    
    // Get table
    localdb::Table* users_table = db.getTable("users");
    ASSERT_NE(users_table, nullptr);
    
    // Insert data into table
    localdb::Row user1 = createUserRow(1, "Alice", 25);
    EXPECT_TRUE(users_table->insert(user1));
    
    localdb::Row user2 = createUserRow(2, "Bob", 30);
    EXPECT_TRUE(users_table->insert(user2));
    
    // Select from table
    auto all_users = users_table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_users.size(), 2);
    
    // Drop table
    EXPECT_TRUE(db.dropTable("users"));
}

// Test Database with multiple tables
TEST_F(DatabaseTest, MultipleTablesInteraction) {
    localdb::Database db;
    
    // Create tables
    EXPECT_TRUE(db.createTable("users", user_columns));
    EXPECT_TRUE(db.createTable("products", product_columns));
    
    // Get tables
    localdb::Table* users_table = db.getTable("users");
    localdb::Table* products_table = db.getTable("products");
    
    ASSERT_NE(users_table, nullptr);
    ASSERT_NE(products_table, nullptr);
    
    // Insert data into tables
    users_table->insert(createUserRow(1, "Alice", 25));
    users_table->insert(createUserRow(2, "Bob", 30));
    
    products_table->insert(createProductRow(101, "Laptop", 999.99));
    products_table->insert(createProductRow(102, "Phone", 499.99));
    products_table->insert(createProductRow(103, "Tablet", 299.99));
    
    // Check data in each table
    auto all_users = users_table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_users.size(), 2);
    
    auto all_products = products_table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_products.size(), 3);
    
    // Drop one table
    EXPECT_TRUE(db.dropTable("products"));
    
    // Check remaining table
    EXPECT_EQ(db.getTable("products"), nullptr);
    
    users_table = db.getTable("users");
    ASSERT_NE(users_table, nullptr);
    
    all_users = users_table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_users.size(), 2);
}

// Test Database Transactions
TEST_F(DatabaseTest, Transactions) {
    localdb::Database db;
    
    // Create table
    EXPECT_TRUE(db.createTable("users", user_columns));
    
    // Begin a transaction
    auto transaction = db.beginTransaction();
    ASSERT_NE(transaction, nullptr);
    
    // Insert data through transaction
    EXPECT_TRUE(transaction->insert("users", createUserRow(1, "Alice", 25)));
    EXPECT_TRUE(transaction->insert("users", createUserRow(2, "Bob", 30)));
    
    // Check data before commit
    auto count = transaction->select("users", [](const localdb::Row&) {
        return true;
    }).size();
    EXPECT_EQ(count, 2);
    
    // Commit the transaction
    EXPECT_TRUE(transaction->commit());
    
    // Verify data is accessible after commit
    auto table = db.getTable("users");
    auto all_users = table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_users.size(), 2);
}

// Test Transaction Rollback
TEST_F(DatabaseTest, TransactionRollback) {
    localdb::Database db;
    
    // Create table
    EXPECT_TRUE(db.createTable("users", user_columns));
    
    // Insert initial data
    {
        auto tx = db.beginTransaction();
        tx->insert("users", createUserRow(1, "Alice", 25));
        tx->commit();
    }
    
    // Begin a transaction
    auto transaction = db.beginTransaction();
    
    // Insert more data
    EXPECT_TRUE(transaction->insert("users", createUserRow(2, "Bob", 30)));
    EXPECT_TRUE(transaction->insert("users", createUserRow(3, "Charlie", 35)));
    
    // Check data before rollback
    auto count = transaction->select("users", [](const localdb::Row&) {
        return true;
    }).size();
    EXPECT_EQ(count, 3);
    
    // Rollback the transaction
    transaction->rollback();
    
    // Verify data is rolled back
    auto table = db.getTable("users");
    auto all_users = table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_users.size(), 1); // Only the initial insert should remain
}

// Test Transaction Auto-Rollback
TEST_F(DatabaseTest, TransactionAutoRollback) {
    localdb::Database db;
    
    // Create table
    EXPECT_TRUE(db.createTable("users", user_columns));
    
    // Insert initial data
    {
        auto tx = db.beginTransaction();
        tx->insert("users", createUserRow(1, "Alice", 25));
        tx->commit();
    }
    
    // Begin a transaction with scope
    {
        auto transaction = db.beginTransaction();
        
        // Insert more data
        EXPECT_TRUE(transaction->insert("users", createUserRow(2, "Bob", 30)));
        
        // Transaction will go out of scope without commit, should auto-rollback
    }
    
    // Verify data is rolled back
    auto table = db.getTable("users");
    auto all_users = table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_users.size(), 1); // Only the initial insert should remain
}

// Test Transaction Update and Delete
TEST_F(DatabaseTest, TransactionUpdateAndDelete) {
    localdb::Database db;
    
    // Create table
    EXPECT_TRUE(db.createTable("users", user_columns));
    
    // Insert initial data
    {
        auto tx = db.beginTransaction();
        tx->insert("users", createUserRow(1, "Alice", 25));
        tx->insert("users", createUserRow(2, "Bob", 30));
        tx->insert("users", createUserRow(3, "Charlie", 35));
        tx->commit();
    }
    
    // Begin a transaction for updates
    auto transaction = db.beginTransaction();
    
    // Update a row
    EXPECT_TRUE(transaction->update("users", 
        createUserRow(2, "Bobby", 31), 
        [](const localdb::Row& row) {
            return row[0].asInt() == 2;
        }
    ));
    
    // Delete a row
    EXPECT_TRUE(transaction->remove("users", 
        [](const localdb::Row& row) {
            return row[0].asInt() == 3;
        }
    ));
    
    // Commit the transaction
    EXPECT_TRUE(transaction->commit());
    
    // Verify updates and deletes
    auto table = db.getTable("users");
    
    // Check total count
    auto all_users = table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_users.size(), 2); // 3 initial - 1 deleted
    
    // Check updated row
    auto bob_rows = table->select([](const localdb::Row& row) {
        return row[0].asInt() == 2;
    });
    EXPECT_EQ(bob_rows.size(), 1);
    EXPECT_EQ(bob_rows[0][1].asText(), "Bobby");
    EXPECT_EQ(bob_rows[0][2].asInt(), 31);
    
    // Check deleted row
    auto charlie_rows = table->select([](const localdb::Row& row) {
        return row[0].asInt() == 3;
    });
    EXPECT_EQ(charlie_rows.size(), 0);
}

// Test for database disk operations
TEST_F(DatabaseTest, DiskOperations) {
    localdb::Database db;
    const std::string test_file = "test_database.bin";
    
    // Create tables
    EXPECT_TRUE(db.createTable("users", user_columns));
    EXPECT_TRUE(db.createTable("products", product_columns));
    
    // Insert data into tables
    auto transaction = db.beginTransaction();
    
    // Insert users
    for (int i = 1; i <= 5; i++) {
        transaction->insert("users", createUserRow(i, "User " + std::to_string(i), 20 + i));
    }
    
    // Insert products
    for (int i = 101; i <= 105; i++) {
        transaction->insert("products", createProductRow(i, "Product " + std::to_string(i), i * 10.5));
    }
    
    transaction->commit();
    
    // Save database to file
    EXPECT_TRUE(db.saveToFile(test_file));
    
    // Create a new database instance
    localdb::Database db2;
    
    // Load database from file
    EXPECT_TRUE(db2.loadFromFile(test_file));
    
    // Verify tables were loaded correctly
    auto table_names = db2.getTableNames();
    EXPECT_EQ(table_names.size(), 2);
    
    // Check if specific tables exist
    bool has_users = false;
    bool has_products = false;
    
    for (const auto& name : table_names) {
        if (name == "users") has_users = true;
        if (name == "products") has_products = true;
    }
    
    EXPECT_TRUE(has_users);
    EXPECT_TRUE(has_products);
    
    // Verify data in tables
    auto tx = db2.beginTransaction();
    
    // Check users
    auto users = tx->select("users", [](const localdb::Row&) { return true; });
    EXPECT_EQ(users.size(), 5);
    
    // Check first user
    auto user1 = tx->select("users", [](const localdb::Row& row) { 
        return row[0].asInt() == 1; 
    });
    EXPECT_EQ(user1.size(), 1);
    EXPECT_EQ(user1[0][1].asText(), "User 1");
    EXPECT_EQ(user1[0][2].asInt(), 21);
    
    // Check products
    auto products = tx->select("products", [](const localdb::Row&) { return true; });
    EXPECT_EQ(products.size(), 5);
    
    // Check first product
    auto product1 = tx->select("products", [](const localdb::Row& row) { 
        return row[0].asInt() == 101; 
    });
    EXPECT_EQ(product1.size(), 1);
    EXPECT_EQ(product1[0][1].asText(), "Product 101");
    EXPECT_DOUBLE_EQ(product1[0][2].asFloat(), 1060.5);
    
    tx->commit();
    
    // Clean up
    std::remove(test_file.c_str());
}

// Test for disk operations with empty database
TEST_F(DatabaseTest, EmptyDatabaseDiskOperations) {
    localdb::Database db;
    const std::string test_file = "empty_database.bin";
    
    // Save empty database to file
    EXPECT_TRUE(db.saveToFile(test_file));
    
    // Create a new database instance
    localdb::Database db2;
    
    // Load database from file
    EXPECT_TRUE(db2.loadFromFile(test_file));
    
    // Verify no tables were loaded
    auto table_names = db2.getTableNames();
    EXPECT_EQ(table_names.size(), 0);
    
    // Clean up
    std::remove(test_file.c_str());
}

// Test for disk operations with invalid file
TEST_F(DatabaseTest, InvalidFileDiskOperations) {
    localdb::Database db;
    
    // Try to load from non-existent file
    EXPECT_FALSE(db.loadFromFile("non_existent_file.bin"));
    
    // Try to load from invalid file
    const std::string invalid_file = "invalid_file.bin";
    
    // Create an invalid file
    {
        std::ofstream file(invalid_file, std::ios::binary);
        file << "This is not a valid database file";
    }
    
    // Try to load it
    EXPECT_FALSE(db.loadFromFile(invalid_file));
    
    // Clean up
    std::remove(invalid_file.c_str());
}

// Test for database modifications after loading
TEST_F(DatabaseTest, ModifyAfterLoading) {
    localdb::Database db;
    const std::string test_file = "modify_test.bin";
    
    // Create and populate a table
    EXPECT_TRUE(db.createTable("users", user_columns));
    
    auto tx = db.beginTransaction();
    tx->insert("users", createUserRow(1, "Original User", 25));
    tx->commit();
    
    // Save database to file
    EXPECT_TRUE(db.saveToFile(test_file));
    
    // Create a new database instance and load the file
    localdb::Database db2;
    EXPECT_TRUE(db2.loadFromFile(test_file));
    
    // Modify the loaded database
    auto tx2 = db2.beginTransaction();
    
    // Add a new row
    tx2->insert("users", createUserRow(2, "New User", 30));
    
    // Update existing row
    tx2->update("users", 
        createUserRow(1, "Updated User", 26), 
        [](const localdb::Row& row) { return row[0].asInt() == 1; }
    );
    
    tx2->commit();
    
    // Verify changes
    auto tx3 = db2.beginTransaction();
    
    // Check total count
    auto all_users = tx3->select("users", [](const localdb::Row&) { return true; });
    EXPECT_EQ(all_users.size(), 2);
    
    // Check updated user
    auto updated_user = tx3->select("users", [](const localdb::Row& row) { 
        return row[0].asInt() == 1; 
    });
    EXPECT_EQ(updated_user.size(), 1);
    EXPECT_EQ(updated_user[0][1].asText(), "Updated User");
    EXPECT_EQ(updated_user[0][2].asInt(), 26);
    
    // Check new user
    auto new_user = tx3->select("users", [](const localdb::Row& row) { 
        return row[0].asInt() == 2; 
    });
    EXPECT_EQ(new_user.size(), 1);
    EXPECT_EQ(new_user[0][1].asText(), "New User");
    EXPECT_EQ(new_user[0][2].asInt(), 30);
    
    tx3->commit();
    
    // Save the modified database
    EXPECT_TRUE(db2.saveToFile(test_file));
    
    // Load it again to verify persistence
    localdb::Database db3;
    EXPECT_TRUE(db3.loadFromFile(test_file));
    
    // Verify all changes were saved
    auto tx4 = db3.beginTransaction();
    
    auto all_users_reloaded = tx4->select("users", [](const localdb::Row&) { return true; });
    EXPECT_EQ(all_users_reloaded.size(), 2);
    
    tx4->commit();
    
    // Clean up
    std::remove(test_file.c_str());
}

}  // namespace 