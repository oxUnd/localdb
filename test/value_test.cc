#include <gtest/gtest.h>
#include "localdb.h"
#include <string>
#include <vector>
#include <stdexcept>

namespace {

class ValueTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Set up code
    }

    void TearDown() override {
        // Tear down code
    }
};

// Test Value constructor and type system
TEST_F(ValueTest, ConstructorAndType) {
    // Null type
    localdb::Value null_val;
    EXPECT_EQ(null_val.type, localdb::Value::NULL_TYPE);
    
    // Int type
    localdb::Value int_val(42);
    EXPECT_EQ(int_val.type, localdb::Value::INT);
    EXPECT_EQ(int_val.asInt(), 42);
    
    // Float type
    localdb::Value float_val(3.14);
    EXPECT_EQ(float_val.type, localdb::Value::FLOAT);
    EXPECT_DOUBLE_EQ(float_val.asFloat(), 3.14);
    
    // Text type
    std::string text = "Hello, World!";
    localdb::Value text_val(text);
    EXPECT_EQ(text_val.type, localdb::Value::TEXT);
    EXPECT_EQ(text_val.asText(), "Hello, World!");
    
    // Blob type
    std::vector<uint8_t> blob = {0x00, 0x01, 0x02, 0x03};
    localdb::Value blob_val(blob);
    EXPECT_EQ(blob_val.type, localdb::Value::BLOB);
    EXPECT_EQ(blob_val.asBlob(), blob);
}

// Test Value copy constructor and assignment operator
TEST_F(ValueTest, CopyConstructorAndAssignment) {
    // Original values
    localdb::Value int_val(42);
    localdb::Value float_val(3.14);
    localdb::Value text_val(std::string("Hello"));
    std::vector<uint8_t> blob = {0x00, 0x01, 0x02, 0x03};
    localdb::Value blob_val(blob);
    
    // Copy construction
    localdb::Value int_copy(int_val);
    localdb::Value float_copy(float_val);
    localdb::Value text_copy(text_val);
    localdb::Value blob_copy(blob_val);
    
    // Test that copies have correct types and values
    EXPECT_EQ(int_copy.type, localdb::Value::INT);
    EXPECT_EQ(int_copy.asInt(), 42);
    
    EXPECT_EQ(float_copy.type, localdb::Value::FLOAT);
    EXPECT_DOUBLE_EQ(float_copy.asFloat(), 3.14);
    
    EXPECT_EQ(text_copy.type, localdb::Value::TEXT);
    EXPECT_EQ(text_copy.asText(), "Hello");
    
    EXPECT_EQ(blob_copy.type, localdb::Value::BLOB);
    EXPECT_EQ(blob_copy.asBlob(), blob);
    
    // Assignment
    localdb::Value int_assign;
    int_assign = int_val;
    EXPECT_EQ(int_assign.type, localdb::Value::INT);
    EXPECT_EQ(int_assign.asInt(), 42);
    
    localdb::Value text_assign;
    text_assign = text_val;
    EXPECT_EQ(text_assign.type, localdb::Value::TEXT);
    EXPECT_EQ(text_assign.asText(), "Hello");
}

// Test Value move constructor and assignment operator
TEST_F(ValueTest, MoveConstructorAndAssignment) {
    // Create values to move from
    localdb::Value int_val(42);
    localdb::Value text_val(std::string("Hello"));
    
    // Move construction
    localdb::Value int_move(std::move(int_val));
    EXPECT_EQ(int_move.type, localdb::Value::INT);
    EXPECT_EQ(int_move.asInt(), 42);
    EXPECT_EQ(int_val.type, localdb::Value::NULL_TYPE); // Moved-from should be null
    
    // Move assignment
    localdb::Value text_move;
    text_move = std::move(text_val);
    EXPECT_EQ(text_move.type, localdb::Value::TEXT);
    EXPECT_EQ(text_move.asText(), "Hello");
    EXPECT_EQ(text_val.type, localdb::Value::NULL_TYPE); // Moved-from should be null
}

// Test Value comparison operators
TEST_F(ValueTest, ComparisonOperators) {
    // Create values to compare
    localdb::Value int_val1(42);
    localdb::Value int_val2(42);
    localdb::Value int_val3(43);
    
    localdb::Value float_val1(3.14);
    localdb::Value float_val2(3.14);
    localdb::Value float_val3(2.71);
    
    localdb::Value text_val1(std::string("Hello"));
    localdb::Value text_val2(std::string("Hello"));
    localdb::Value text_val3(std::string("World"));
    
    // Equality operator (==)
    EXPECT_TRUE(int_val1 == int_val2);
    EXPECT_FALSE(int_val1 == int_val3);
    
    EXPECT_TRUE(float_val1 == float_val2);
    EXPECT_FALSE(float_val1 == float_val3);
    
    EXPECT_TRUE(text_val1 == text_val2);
    EXPECT_FALSE(text_val1 == text_val3);
    
    // Different types should not be equal
    EXPECT_FALSE(int_val1 == float_val1);
    EXPECT_FALSE(int_val1 == text_val1);
    
    // Inequality operator (!=)
    EXPECT_FALSE(int_val1 != int_val2);
    EXPECT_TRUE(int_val1 != int_val3);
    
    // Less than operator (<)
    EXPECT_FALSE(int_val1 < int_val2);
    EXPECT_TRUE(int_val1 < int_val3);
    EXPECT_FALSE(int_val3 < int_val1);
    
    EXPECT_TRUE(float_val3 < float_val1);
    EXPECT_TRUE(text_val1 < text_val3);
}

// Test Value type exceptions
TEST_F(ValueTest, TypeExceptions) {
    localdb::Value int_val(42);
    localdb::Value float_val(3.14);
    localdb::Value text_val(std::string("Hello"));
    
    // Attempt to access with wrong type should throw
    EXPECT_THROW(int_val.asFloat(), std::runtime_error);
    EXPECT_THROW(int_val.asText(), std::runtime_error);
    EXPECT_THROW(int_val.asBlob(), std::runtime_error);
    
    EXPECT_THROW(float_val.asInt(), std::runtime_error);
    EXPECT_THROW(float_val.asText(), std::runtime_error);
    EXPECT_THROW(float_val.asBlob(), std::runtime_error);
    
    EXPECT_THROW(text_val.asInt(), std::runtime_error);
    EXPECT_THROW(text_val.asFloat(), std::runtime_error);
    EXPECT_THROW(text_val.asBlob(), std::runtime_error);
}

}  // namespace 