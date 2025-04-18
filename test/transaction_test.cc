#include <gtest/gtest.h>
#include "localdb.h"
#include <string>
#include <vector>
#include <stdexcept>
#include <thread>
#include <future>

namespace {

class TransactionTest : public ::testing::Test {
protected:
    localdb::Database db;
    std::vector<localdb::Column> user_columns;
    
    void SetUp() override {
        user_columns = {
            {"id", localdb::Column::INT, true, true, true},      // Primary key
            {"name", localdb::Column::TEXT, false, true, false}, // Not null
            {"age", localdb::Column::INT, false, false, false}   // Regular column
        };
        
        db.createTable("users", user_columns);
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
};

// Test Transaction Basics
TEST_F(TransactionTest, TransactionBasics) {
    // Begin a transaction
    auto transaction = db.beginTransaction();
    ASSERT_NE(transaction, nullptr);
    
    // Transaction should be active
    EXPECT_TRUE(transaction->commit());
    
    // After commit, transaction should not be active
    EXPECT_FALSE(transaction->commit());
    EXPECT_FALSE(transaction->insert("users", createUserRow(1, "Alice", 25)));
}

// Test Transaction Insert
TEST_F(TransactionTest, TransactionInsert) {
    // Begin a transaction
    auto transaction = db.beginTransaction();
    
    // Insert data
    EXPECT_TRUE(transaction->insert("users", createUserRow(1, "Alice", 25)));
    EXPECT_TRUE(transaction->insert("users", createUserRow(2, "Bob", 30)));
    
    // Should not be able to insert with duplicate primary key
    EXPECT_FALSE(transaction->insert("users", createUserRow(1, "Duplicate", 40)));
    
    // Commit the transaction
    EXPECT_TRUE(transaction->commit());
    
    // Verify data
    auto table = db.getTable("users");
    auto all_users = table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_users.size(), 2);
}

// Test Transaction Select
TEST_F(TransactionTest, TransactionSelect) {
    // Insert some data
    {
        auto tx = db.beginTransaction();
        tx->insert("users", createUserRow(1, "Alice", 25));
        tx->insert("users", createUserRow(2, "Bob", 30));
        tx->insert("users", createUserRow(3, "Charlie", 35));
        tx->commit();
    }
    
    // Begin a transaction
    auto transaction = db.beginTransaction();
    
    // Select all rows
    auto all_rows = transaction->select("users", [](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_rows.size(), 3);
    
    // Select rows with id > 1
    auto filtered_rows = transaction->select("users", [](const localdb::Row& row) {
        return row[0].asInt() > 1;
    });
    EXPECT_EQ(filtered_rows.size(), 2);
    EXPECT_EQ(filtered_rows[0][0].asInt(), 2);
    EXPECT_EQ(filtered_rows[1][0].asInt(), 3);
    
    // Select from non-existent table
    auto no_rows = transaction->select("non_existent", [](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(no_rows.size(), 0);
    
    transaction->commit();
}

// Test Transaction Update
TEST_F(TransactionTest, TransactionUpdate) {
    // Insert some data
    {
        auto tx = db.beginTransaction();
        tx->insert("users", createUserRow(1, "Alice", 25));
        tx->insert("users", createUserRow(2, "Bob", 30));
        tx->insert("users", createUserRow(3, "Charlie", 35));
        tx->commit();
    }
    
    // Begin a transaction
    auto transaction = db.beginTransaction();
    
    // Update Bob's age
    EXPECT_TRUE(transaction->update("users", 
        createUserRow(2, "Bob", 31), 
        [](const localdb::Row& row) {
            return row[0].asInt() == 2;
        }
    ));
    
    // Commit the transaction
    EXPECT_TRUE(transaction->commit());
    
    // Verify the update
    auto table = db.getTable("users");
    auto bob_rows = table->select([](const localdb::Row& row) {
        return row[0].asInt() == 2;
    });
    EXPECT_EQ(bob_rows.size(), 1);
    EXPECT_EQ(bob_rows[0][1].asText(), "Bob");
    EXPECT_EQ(bob_rows[0][2].asInt(), 31);
}

// Test Transaction Remove
TEST_F(TransactionTest, TransactionRemove) {
    // Insert some data
    {
        auto tx = db.beginTransaction();
        tx->insert("users", createUserRow(1, "Alice", 25));
        tx->insert("users", createUserRow(2, "Bob", 30));
        tx->insert("users", createUserRow(3, "Charlie", 35));
        tx->commit();
    }
    
    // Begin a transaction
    auto transaction = db.beginTransaction();
    
    // Remove Bob
    EXPECT_TRUE(transaction->remove("users", 
        [](const localdb::Row& row) {
            return row[0].asInt() == 2;
        }
    ));
    
    // Commit the transaction
    EXPECT_TRUE(transaction->commit());
    
    // Verify the removal
    auto table = db.getTable("users");
    auto bob_rows = table->select([](const localdb::Row& row) {
        return row[0].asInt() == 2;
    });
    EXPECT_EQ(bob_rows.size(), 0);
    
    auto all_rows = table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_rows.size(), 2);
}

// Test Transaction Rollback
TEST_F(TransactionTest, TransactionRollback) {
    // Insert some data
    {
        auto tx = db.beginTransaction();
        tx->insert("users", createUserRow(1, "Alice", 25));
        tx->commit();
    }
    
    // Begin a transaction
    auto transaction = db.beginTransaction();
    
    // Insert more data
    EXPECT_TRUE(transaction->insert("users", createUserRow(2, "Bob", 30)));
    
    // Update Alice's age
    EXPECT_TRUE(transaction->update("users", 
        createUserRow(1, "Alice", 26), 
        [](const localdb::Row& row) {
            return row[0].asInt() == 1;
        }
    ));
    
    // Rollback the transaction
    transaction->rollback();
    
    // Verify rollback
    auto table = db.getTable("users");
    
    // Should only have Alice
    auto all_rows = table->select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_rows.size(), 1);
    
    // Alice's age should still be 25
    auto alice_rows = table->select([](const localdb::Row& row) {
        return row[0].asInt() == 1;
    });
    EXPECT_EQ(alice_rows.size(), 1);
    EXPECT_EQ(alice_rows[0][2].asInt(), 25);
}

// Test Transaction Concurrency
TEST_F(TransactionTest, TransactionConcurrency) {
    // Insert initial data
    {
        auto tx = db.beginTransaction();
        tx->insert("users", createUserRow(1, "Alice", 25));
        tx->commit();
    }
    
    // Function to run in thread 1
    auto thread1_func = [this]() {
        auto tx = db.beginTransaction();
        
        // Read all users
        auto initial_rows = tx->select("users", [](const localdb::Row&) {
            return true;
        });
        
        // Insert a new user
        tx->insert("users", createUserRow(2, "Bob", 30));
        
        // Update Alice
        tx->update("users", 
            createUserRow(1, "Alice Updated", 26), 
            [](const localdb::Row& row) {
                return row[0].asInt() == 1;
            }
        );
        
        // Sleep to simulate longer transaction
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // Commit
        tx->commit();
        
        return initial_rows.size();
    };
    
    // Function to run in thread 2
    auto thread2_func = [this]() {
        auto tx = db.beginTransaction();
        
        // Read all users
        auto initial_rows = tx->select("users", [](const localdb::Row&) {
            return true;
        });
        
        // Insert a different user
        tx->insert("users", createUserRow(3, "Charlie", 35));
        
        // Sleep to simulate longer transaction
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        
        // Commit
        tx->commit();
        
        return initial_rows.size();
    };
    
    // Start the threads
    auto future1 = std::async(std::launch::async, thread1_func);
    auto future2 = std::async(std::launch::async, thread2_func);
    
    // Wait for results
    int initial_count1 = future1.get();
    int initial_count2 = future2.get();
    
    // Both threads should have seen at least the initial data
    EXPECT_GE(initial_count1, 1);
    EXPECT_GE(initial_count2, 1);
    
    // Verify final state
    auto table = db.getTable("users");
    auto all_rows = table->select([](const localdb::Row&) {
        return true;
    });
    
    // Should have 3 rows now
    EXPECT_EQ(all_rows.size(), 3);
    
    // Alice should be updated
    auto alice_rows = table->select([](const localdb::Row& row) {
        return row[0].asInt() == 1;
    });
    EXPECT_EQ(alice_rows.size(), 1);
    EXPECT_EQ(alice_rows[0][1].asText(), "Alice Updated");
    EXPECT_EQ(alice_rows[0][2].asInt(), 26);
    
    // Bob and Charlie should be inserted
    auto bob_rows = table->select([](const localdb::Row& row) {
        return row[0].asInt() == 2;
    });
    EXPECT_EQ(bob_rows.size(), 1);
    
    auto charlie_rows = table->select([](const localdb::Row& row) {
        return row[0].asInt() == 3;
    });
    EXPECT_EQ(charlie_rows.size(), 1);
}

// Test Transaction with Invalid Table
TEST_F(TransactionTest, TransactionInvalidTable) {
    auto transaction = db.beginTransaction();
    
    // Operations on non-existent table should fail
    EXPECT_FALSE(transaction->insert("non_existent", createUserRow(1, "Alice", 25)));
    EXPECT_FALSE(transaction->update("non_existent", 
        createUserRow(1, "Alice", 26), 
        [](const localdb::Row& row) {
            return row[0].asInt() == 1;
        }
    ));
    EXPECT_FALSE(transaction->remove("non_existent", 
        [](const localdb::Row& row) {
            return row[0].asInt() == 1;
        }
    ));
    
    auto rows = transaction->select("non_existent", [](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(rows.size(), 0);
    
    transaction->commit();
}

}  // namespace 