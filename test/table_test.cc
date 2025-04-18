#include <gtest/gtest.h>
#include "localdb.h"
#include <string>
#include <vector>
#include <stdexcept>
#include <thread>
#include <future>

namespace {

class TableTest : public ::testing::Test {
protected:
    std::vector<localdb::Column> columns;
    
    void SetUp() override {
        columns = {
            {"id", localdb::Column::INT, true, true, true},     // Primary key
            {"name", localdb::Column::TEXT, false, true, false}, // Not null
            {"age", localdb::Column::INT, false, false, false}   // Regular column
        };
    }

    void TearDown() override {
        // Any cleanup
    }
    
    localdb::Row createRow(int id, const std::string& name, int age) {
        localdb::Row row;
        row.push_back(localdb::Value(id));
        row.push_back(localdb::Value(name));
        row.push_back(localdb::Value(age));
        return row;
    }
};

// Test Table creation
TEST_F(TableTest, TableCreation) {
    localdb::Table table("test_table", columns);
    
    EXPECT_EQ(table.getName(), "test_table");
    EXPECT_EQ(table.getColumns().size(), 3);
    
    // Check columns structure
    auto& cols = table.getColumns();
    EXPECT_EQ(cols[0].name, "id");
    EXPECT_EQ(cols[0].type, localdb::Column::INT);
    EXPECT_TRUE(cols[0].primary_key);
    
    EXPECT_EQ(cols[1].name, "name");
    EXPECT_EQ(cols[1].type, localdb::Column::TEXT);
    EXPECT_FALSE(cols[1].primary_key);
    EXPECT_TRUE(cols[1].not_null);
    
    EXPECT_EQ(cols[2].name, "age");
    EXPECT_EQ(cols[2].type, localdb::Column::INT);
    EXPECT_FALSE(cols[2].primary_key);
}

// Test for multiple primary keys (should throw)
TEST_F(TableTest, MultiplePrimaryKeys) {
    std::vector<localdb::Column> invalid_columns = {
        {"id1", localdb::Column::INT, true, true, true},
        {"id2", localdb::Column::INT, true, true, true},
        {"name", localdb::Column::TEXT, false, true, false}
    };
    
    EXPECT_THROW(localdb::Table("invalid_table", invalid_columns), std::runtime_error);
}

// Test Table insertion
TEST_F(TableTest, TableInsert) {
    localdb::Table table("test_table", columns);
    
    // Valid insert
    localdb::Row row1 = createRow(1, "Alice", 25);
    EXPECT_TRUE(table.insert(row1));
    
    // Invalid insert - duplicate primary key
    localdb::Row row2 = createRow(1, "Bob", 30);
    EXPECT_FALSE(table.insert(row2));
    
    // Valid insert - different primary key
    localdb::Row row3 = createRow(2, "Charlie", 35);
    EXPECT_TRUE(table.insert(row3));
    
    // Invalid insert - wrong number of columns
    localdb::Row invalid_row;
    invalid_row.push_back(localdb::Value(3));
    invalid_row.push_back(localdb::Value(std::string("Dave")));
    // Missing age column
    EXPECT_FALSE(table.insert(invalid_row));
}

// Test Table selection
TEST_F(TableTest, TableSelect) {
    localdb::Table table("test_table", columns);
    
    // Insert some data
    table.insert(createRow(1, "Alice", 25));
    table.insert(createRow(2, "Bob", 30));
    table.insert(createRow(3, "Charlie", 35));
    table.insert(createRow(4, "Dave", 40));
    
    // Select all rows
    auto all_rows = table.select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_rows.size(), 4);
    
    // Select rows with id > 2
    auto filtered_rows = table.select([](const localdb::Row& row) {
        return row[0].asInt() > 2;
    });
    EXPECT_EQ(filtered_rows.size(), 2);
    EXPECT_EQ(filtered_rows[0][0].asInt(), 3);
    EXPECT_EQ(filtered_rows[1][0].asInt(), 4);
    
    // Select row with name "Bob"
    auto bob_rows = table.select([](const localdb::Row& row) {
        return row[1].asText() == "Bob";
    });
    EXPECT_EQ(bob_rows.size(), 1);
    EXPECT_EQ(bob_rows[0][0].asInt(), 2);
    EXPECT_EQ(bob_rows[0][1].asText(), "Bob");
    EXPECT_EQ(bob_rows[0][2].asInt(), 30);
    
    // Select rows with non-existent criteria
    auto no_rows = table.select([](const localdb::Row& row) {
        return row[0].asInt() > 100;
    });
    EXPECT_EQ(no_rows.size(), 0);
}

// Test Table update
TEST_F(TableTest, TableUpdate) {
    localdb::Table table("test_table", columns);
    
    // Insert some data
    table.insert(createRow(1, "Alice", 25));
    table.insert(createRow(2, "Bob", 30));
    table.insert(createRow(3, "Charlie", 35));
    
    // Update Bob's age
    localdb::Row updated_bob = createRow(2, "Bob", 31);
    bool result = table.update(updated_bob, [](const localdb::Row& row) {
        return row[0].asInt() == 2;
    });
    EXPECT_TRUE(result);
    
    // Verify the update
    auto bob_rows = table.select([](const localdb::Row& row) {
        return row[0].asInt() == 2;
    });
    EXPECT_EQ(bob_rows.size(), 1);
    EXPECT_EQ(bob_rows[0][2].asInt(), 31);
    
    // Update with non-existent criteria
    localdb::Row non_existent = createRow(4, "Dave", 40);
    result = table.update(non_existent, [](const localdb::Row& row) {
        return row[0].asInt() == 4;
    });
    EXPECT_FALSE(result);
    
    // Update with invalid row (wrong column count)
    localdb::Row invalid_row;
    invalid_row.push_back(localdb::Value(1));
    invalid_row.push_back(localdb::Value(std::string("Alice")));
    // Missing age column
    
    result = table.update(invalid_row, [](const localdb::Row& row) {
        return row[0].asInt() == 1;
    });
    EXPECT_FALSE(result);
}

// Test Table remove
TEST_F(TableTest, TableRemove) {
    localdb::Table table("test_table", columns);
    
    // Insert some data
    table.insert(createRow(1, "Alice", 25));
    table.insert(createRow(2, "Bob", 30));
    table.insert(createRow(3, "Charlie", 35));
    
    // Remove Bob
    bool result = table.remove([](const localdb::Row& row) {
        return row[0].asInt() == 2;
    });
    EXPECT_TRUE(result);
    
    // Verify removal
    auto all_rows = table.select([](const localdb::Row&) {
        return true;
    });
    EXPECT_EQ(all_rows.size(), 2);
    
    auto bob_rows = table.select([](const localdb::Row& row) {
        return row[0].asInt() == 2;
    });
    EXPECT_EQ(bob_rows.size(), 0);
    
    // Remove with non-existent criteria
    result = table.remove([](const localdb::Row& row) {
        return row[0].asInt() == 4;
    });
    EXPECT_FALSE(result);
}

// Test multi-threaded table access
TEST_F(TableTest, ThreadedAccess) {
    localdb::Table table("test_table", columns);
    
    // Insert initial data
    table.insert(createRow(1, "Alice", 25));
    
    // Function to add rows
    auto writer_func = [&table](int start_id) {
        for (int i = 0; i < 10; i++) {
            int id = start_id + i;
            table.beginWrite();
            bool result = table.insert(
                {localdb::Value(id), 
                 localdb::Value(std::string("User") + std::to_string(id)), 
                 localdb::Value(20 + id)}
            );
            table.endWrite();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    };
    
    // Function to read rows
    auto reader_func = [&table]() {
        int total_rows = 0;
        for (int i = 0; i < 20; i++) {
            table.beginRead();
            auto rows = table.select([](const localdb::Row&) { return true; });
            total_rows = rows.size();
            table.endRead();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return total_rows;
    };
    
    // Create threads
    std::thread writer1(writer_func, 100);
    std::thread writer2(writer_func, 200);
    
    auto future1 = std::async(std::launch::async, reader_func);
    auto future2 = std::async(std::launch::async, reader_func);
    
    // Wait for threads to complete
    writer1.join();
    writer2.join();
    
    int result1 = future1.get();
    int result2 = future2.get();
    
    // Verify all data was inserted
    auto final_rows = table.select([](const localdb::Row&) { return true; });
    EXPECT_EQ(final_rows.size(), 21); // 1 initial + 10 from writer1 + 10 from writer2
    
    // Reader threads should have seen some data, but maybe not all depending on timing
    EXPECT_GT(result1, 0);
    EXPECT_GT(result2, 0);
}

}  // namespace 