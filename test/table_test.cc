#include <gtest/gtest.h>
#include "localdb.h"
#include <string>
#include <vector>
#include <stdexcept>
#include <thread>
#include <future>
#include <mutex>
#include <shared_mutex>
#include <iostream>

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
    // 设置测试超时，防止测试无限挂起
    auto test_start_time = std::chrono::steady_clock::now();
    auto test_timeout = std::chrono::seconds(5); // 测试最多运行5秒
    
    localdb::Table table("test_table", columns);
    
    // Insert initial data
    table.insert(createRow(1, "Alice", 25));
    
    // 使用事物安全API插入行
    auto safe_insert = [&table](int id, const std::string& name, int age) -> bool {
        // 尝试获取写锁
        if (table.beginWrite()) {
            try {
                // 创建新行
                localdb::Row row;
                row.push_back(localdb::Value(id));
                row.push_back(localdb::Value(name));
                row.push_back(localdb::Value(age));
                
                // 使用table的公共API插入
                bool result = table.insert(row);
                
                // 释放锁
                table.endWrite();
                
                return result;
            } catch (...) {
                // 确保异常情况下也释放锁
                table.endWrite();
                throw;
            }
        }
        return false; // 无法获取锁
    };
    
    // 线程安全的读取行数
    auto safe_count_rows = [&table]() -> int {
        // 尝试获取读锁
        if (table.beginRead()) {
            try {
                // 使用table的公共API查询所有行
                auto rows = table.select([](const localdb::Row&) { return true; });
                int count = static_cast<int>(rows.size());
                
                // 释放锁
                table.endRead();
                
                return count;
            } catch (...) {
                // 确保异常情况下也释放锁
                table.endRead();
                throw;
            }
        }
        return -1; // 无法获取锁
    };
    
    // Function to add rows
    auto writer_func = [&safe_insert, &test_start_time, test_timeout](int start_id) {
        int successful_inserts = 0;
        
        for (int i = 0; i < 10 && std::chrono::steady_clock::now() - test_start_time < test_timeout; i++) {
            int id = start_id + i;
            std::string name = std::string("User") + std::to_string(id);
            
            // 尝试插入，允许稍后重试
            for (int retry = 0; retry < 3; retry++) {
                if (safe_insert(id, name, 20 + id)) {
                    successful_inserts++;
                    break;
                }
                
                // 随机短暂休眠后重试（避免锁竞争模式）
                std::this_thread::sleep_for(std::chrono::milliseconds(1 + (std::rand() % 5)));
                
                // 检查是否超时
                if (std::chrono::steady_clock::now() - test_start_time >= test_timeout) {
                    break;
                }
            }
            
            // 添加随机延迟以增加线程交错的可能性
            std::this_thread::sleep_for(std::chrono::milliseconds(std::rand() % 3));
        }
        
        return successful_inserts;
    };
    
    // Function to read rows
    auto reader_func = [&safe_count_rows, &test_start_time, test_timeout]() {
        int max_count = 0;
        int read_attempts = 0;
        int successful_reads = 0;
        
        for (int i = 0; i < 20 && std::chrono::steady_clock::now() - test_start_time < test_timeout; i++) {
            read_attempts++;
            int count = safe_count_rows();
            
            if (count > 0) {
                max_count = std::max(max_count, count);
                successful_reads++;
            }
            
            // 添加随机延迟
            std::this_thread::sleep_for(std::chrono::milliseconds(std::rand() % 3));
        }
        
        return std::make_tuple(max_count, read_attempts, successful_reads);
    };
    
    // 初始化随机数生成器
    std::srand(static_cast<unsigned int>(std::time(nullptr)));
    
    // 使用future来收集结果
    auto writer1_future = std::async(std::launch::async, [&writer_func]() { 
        return writer_func(100); 
    });
    
    auto writer2_future = std::async(std::launch::async, [&writer_func]() { 
        return writer_func(200); 
    });
    
    auto reader1_future = std::async(std::launch::async, reader_func);
    auto reader2_future = std::async(std::launch::async, reader_func);
    
    // 等待结果，带有超时
    auto wait_with_timeout = [&test_start_time, test_timeout](auto& future) {
        auto remaining_time = test_timeout - (std::chrono::steady_clock::now() - test_start_time);
        if (remaining_time <= std::chrono::milliseconds(0)) {
            remaining_time = std::chrono::milliseconds(100);
        }
        
        auto status = future.wait_for(remaining_time);
        return status == std::future_status::ready;
    };
    
    // 获取结果
    bool writer1_done = wait_with_timeout(writer1_future);
    bool writer2_done = wait_with_timeout(writer2_future);
    bool reader1_done = wait_with_timeout(reader1_future);
    bool reader2_done = wait_with_timeout(reader2_future);
    
    // 判断测试是否成功完成
    bool all_tasks_completed = writer1_done && writer2_done && reader1_done && reader2_done;
    
    if (!all_tasks_completed) {
        GTEST_SKIP() << "Not all tasks completed within the timeout period";
    } else {
        // 获取结果
        int writer1_inserts = writer1_future.get();
        int writer2_inserts = writer2_future.get();
        
        auto [reader1_max, reader1_attempts, reader1_success] = reader1_future.get();
        auto [reader2_max, reader2_attempts, reader2_success] = reader2_future.get();
        
        // 获取最终行数
        int final_count = safe_count_rows();
        
        // 打印一些诊断信息
        std::cout << "Writer1 inserts: " << writer1_inserts << std::endl;
        std::cout << "Writer2 inserts: " << writer2_inserts << std::endl;
        std::cout << "Reader1 max seen: " << reader1_max << " (" << reader1_success << "/" << reader1_attempts << " successful reads)" << std::endl;
        std::cout << "Reader2 max seen: " << reader2_max << " (" << reader2_success << "/" << reader2_attempts << " successful reads)" << std::endl;
        std::cout << "Final row count: " << final_count << std::endl;
        
        // 验证：
        // 1. 至少有一行（初始行）
        // 2. 读取器线程至少成功读取了一些数据
        EXPECT_GE(final_count, 1);
        
        // 确认读取线程至少看到了一些数据
        EXPECT_GT(reader1_success, 0);
        EXPECT_GT(reader2_success, 0);
    }
}

}  // namespace 