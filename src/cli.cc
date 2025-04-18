#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <unistd.h> // For isatty and fileno
#include "localdb.h"

namespace fs = std::filesystem;

class LocalDBCLI {
private:
    localdb::Database db;
    std::string current_db_file;
    bool is_running = true;
    std::shared_ptr<localdb::Transaction> current_transaction;

    // Command handlers
    using CommandHandler = void (LocalDBCLI::*)(const std::vector<std::string>&);
    std::unordered_map<std::string, CommandHandler> command_handlers;
    std::unordered_map<std::string, std::string> command_help;

    // Helper methods
    std::vector<std::string> splitCommand(const std::string& command) {
        std::vector<std::string> tokens;
        std::istringstream iss(command);
        std::string token;
        
        // Handle quoted strings
        while (iss >> std::quoted(token)) {
            tokens.push_back(token);
        }
        
        return tokens;
    }

    localdb::Column::Type parseColumnType(const std::string& type_str) {
        if (type_str == "INT" || type_str == "INTEGER") {
            return localdb::Column::INT;
        } else if (type_str == "FLOAT" || type_str == "REAL") {
            return localdb::Column::FLOAT;
        } else if (type_str == "TEXT" || type_str == "STRING") {
            return localdb::Column::TEXT;
        } else if (type_str == "BLOB") {
            return localdb::Column::BLOB;
        } else {
            throw std::runtime_error("Invalid column type: " + type_str);
        }
    }

    localdb::Value parseValue(const std::string& value_str, localdb::Column::Type type) {
        switch (type) {
            case localdb::Column::INT:
                return localdb::Value(std::stoi(value_str));
            case localdb::Column::FLOAT:
                return localdb::Value(std::stod(value_str));
            case localdb::Column::TEXT:
                return localdb::Value(value_str);
            case localdb::Column::BLOB: {
                // Parse hex string to blob
                std::vector<uint8_t> blob;
                for (size_t i = 0; i < value_str.length(); i += 2) {
                    std::string byte = value_str.substr(i, 2);
                    blob.push_back(static_cast<uint8_t>(std::stoi(byte, nullptr, 16)));
                }
                return localdb::Value(blob);
            }
            default:
                throw std::runtime_error("Unsupported value type");
        }
    }

    void displayRow(const localdb::Row& row, const std::vector<localdb::Column>&) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) std::cout << " | ";
            
            switch (row[i].type) {
                case localdb::Value::INT:
                    std::cout << row[i].asInt();
                    break;
                case localdb::Value::FLOAT:
                    std::cout << row[i].asFloat();
                    break;
                case localdb::Value::TEXT:
                    std::cout << row[i].asText();
                    break;
                case localdb::Value::BLOB: {
                    auto blob = row[i].asBlob();
                    std::cout << "BLOB[" << blob.size() << " bytes]";
                    break;
                }
                default:
                    std::cout << "NULL";
            }
        }
        std::cout << std::endl;
    }

    void displayHeader(const std::vector<localdb::Column>& columns) {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) std::cout << " | ";
            std::cout << columns[i].name;
        }
        std::cout << std::endl;

        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) std::cout << "-+-";
            std::cout << std::string(columns[i].name.length(), '-');
        }
        std::cout << std::endl;
    }

public:
    LocalDBCLI() {
        // Initialize command handlers
        command_handlers["help"] = &LocalDBCLI::handleHelp;
        command_handlers["exit"] = &LocalDBCLI::handleExit;
        command_handlers["quit"] = &LocalDBCLI::handleExit;
        command_handlers["create_table"] = &LocalDBCLI::handleCreateTable;
        command_handlers["drop_table"] = &LocalDBCLI::handleDropTable;
        command_handlers["list_tables"] = &LocalDBCLI::handleListTables;
        command_handlers["describe_table"] = &LocalDBCLI::handleDescribeTable;
        command_handlers["insert"] = &LocalDBCLI::handleInsert;
        command_handlers["select"] = &LocalDBCLI::handleSelect;
        command_handlers["update"] = &LocalDBCLI::handleUpdate;
        command_handlers["delete"] = &LocalDBCLI::handleDelete;
        command_handlers["begin"] = &LocalDBCLI::handleBeginTransaction;
        command_handlers["commit"] = &LocalDBCLI::handleCommitTransaction;
        command_handlers["rollback"] = &LocalDBCLI::handleRollbackTransaction;
        command_handlers["save"] = &LocalDBCLI::handleSaveDatabase;
        command_handlers["load"] = &LocalDBCLI::handleLoadDatabase;
        
        // Initialize command help
        command_help["help"] = "Display help information";
        command_help["exit"] = "Exit the program";
        command_help["quit"] = "Exit the program";
        command_help["create_table"] = "Create a new table. Usage: create_table TABLE_NAME COL1:TYPE:PK:NN:UQ [COL2:TYPE:PK:NN:UQ ...]";
        command_help["drop_table"] = "Drop a table. Usage: drop_table TABLE_NAME";
        command_help["list_tables"] = "List all tables in the database";
        command_help["describe_table"] = "Describe table schema. Usage: describe_table TABLE_NAME";
        command_help["insert"] = "Insert a row into a table. Usage: insert TABLE_NAME VAL1 VAL2 ...";
        command_help["select"] = "Select rows from a table. Usage: select TABLE_NAME [WHERE COL_INDEX OPERATOR VALUE]";
        command_help["update"] = "Update rows in a table. Usage: update TABLE_NAME COL1=VAL1 [COL2=VAL2 ...] WHERE COL_INDEX OPERATOR VALUE";
        command_help["delete"] = "Delete rows from a table. Usage: delete TABLE_NAME WHERE COL_INDEX OPERATOR VALUE";
        command_help["begin"] = "Begin a transaction";
        command_help["commit"] = "Commit the current transaction";
        command_help["rollback"] = "Rollback the current transaction";
        command_help["save"] = "Save the database to a file. Usage: save FILENAME";
        command_help["load"] = "Load the database from a file. Usage: load FILENAME";
    }

    void run() {
        std::cout << "LocalDB CLI" << std::endl;
        std::cout << "Type 'help' for a list of commands" << std::endl;
        
        std::string input;
        bool interactive = isatty(fileno(stdin));
        
        while (is_running) {
            if (interactive) {
                std::cout << "localdb> ";
            }
            
            // Check for EOF
            if (!std::getline(std::cin, input)) {
                // EOF reached
                break;
            }
            
            if (input.empty()) continue;
            
            try {
                processCommand(input);
            } catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << std::endl;
            }
        }
    }

    void processCommand(const std::string& input) {
        auto tokens = splitCommand(input);
        if (tokens.empty()) return;
        
        std::string cmd = tokens[0];
        auto it = command_handlers.find(cmd);
        
        if (it != command_handlers.end()) {
            // Remove the command from tokens
            tokens.erase(tokens.begin());
            
            // Call the handler
            (this->*(it->second))(tokens);
        } else {
            std::cout << "Unknown command: " << cmd << std::endl;
            std::cout << "Type 'help' for a list of commands" << std::endl;
        }
    }

    // Command handler implementations
    void handleHelp(const std::vector<std::string>& args) {
        if (args.empty()) {
            std::cout << "Available commands:" << std::endl;
            for (const auto& [cmd, help] : command_help) {
                std::cout << "  " << cmd << " - " << help << std::endl;
            }
        } else {
            auto it = command_help.find(args[0]);
            if (it != command_help.end()) {
                std::cout << it->first << " - " << it->second << std::endl;
            } else {
                std::cout << "No help available for '" << args[0] << "'" << std::endl;
            }
        }
    }

    void handleExit(const std::vector<std::string>&) {
        is_running = false;
        std::cout << "Exiting LocalDB CLI" << std::endl;
    }

    void handleCreateTable(const std::vector<std::string>& args) {
        if (args.size() < 2) {
            std::cout << "Usage: create_table TABLE_NAME COL1:TYPE:PK:NN:UQ [COL2:TYPE:PK:NN:UQ ...]" << std::endl;
            return;
        }
        
        std::string table_name = args[0];
        std::vector<localdb::Column> columns;
        
        for (size_t i = 1; i < args.size(); ++i) {
            std::string column_def = args[i];
            std::istringstream iss(column_def);
            std::string part;
            std::vector<std::string> parts;
            
            while (std::getline(iss, part, ':')) {
                parts.push_back(part);
            }
            
            if (parts.size() < 2) {
                throw std::runtime_error("Invalid column definition: " + column_def);
            }
            
            localdb::Column column;
            column.name = parts[0];
            column.type = parseColumnType(parts[1]);
            
            // Parse optional attributes
            if (parts.size() > 2) column.primary_key = (parts[2] == "PK" || parts[2] == "1" || parts[2] == "true");
            if (parts.size() > 3) column.not_null = (parts[3] == "NN" || parts[3] == "1" || parts[3] == "true");
            if (parts.size() > 4) column.unique = (parts[4] == "UQ" || parts[4] == "1" || parts[4] == "true");
            
            columns.push_back(column);
        }
        
        bool success = db.createTable(table_name, columns);
        if (success) {
            std::cout << "Table '" << table_name << "' created successfully" << std::endl;
        } else {
            std::cout << "Failed to create table '" << table_name << "'" << std::endl;
        }
    }

    void handleDropTable(const std::vector<std::string>& args) {
        if (args.size() != 1) {
            std::cout << "Usage: drop_table TABLE_NAME" << std::endl;
            return;
        }
        
        std::string table_name = args[0];
        bool success = db.dropTable(table_name);
        
        if (success) {
            std::cout << "Table '" << table_name << "' dropped successfully" << std::endl;
        } else {
            std::cout << "Failed to drop table '" << table_name << "'" << std::endl;
        }
    }

    void handleListTables(const std::vector<std::string>&) {
        auto tables = db.getTableNames();
        
        if (tables.empty()) {
            std::cout << "No tables in database" << std::endl;
            return;
        }
        
        std::cout << "Tables in database:" << std::endl;
        for (const auto& table : tables) {
            std::cout << "  " << table << std::endl;
        }
    }

    void handleDescribeTable(const std::vector<std::string>& args) {
        if (args.size() != 1) {
            std::cout << "Usage: describe_table TABLE_NAME" << std::endl;
            return;
        }
        
        std::string table_name = args[0];
        localdb::Table* table = db.getTable(table_name);
        
        if (!table) {
            std::cout << "Table '" << table_name << "' does not exist" << std::endl;
            return;
        }
        
        const auto& columns = table->getColumns();
        
        std::cout << "Table '" << table_name << "':" << std::endl;
        std::cout << "  Column Name | Type | Primary Key | Not Null | Unique" << std::endl;
        std::cout << "  ------------------------------------" << std::endl;
        
        for (const auto& col : columns) {
            std::string type_str;
            switch (col.type) {
                case localdb::Column::INT: type_str = "INT"; break;
                case localdb::Column::FLOAT: type_str = "FLOAT"; break;
                case localdb::Column::TEXT: type_str = "TEXT"; break;
                case localdb::Column::BLOB: type_str = "BLOB"; break;
            }
            
            std::cout << "  " << col.name << " | " 
                      << type_str << " | "
                      << (col.primary_key ? "Yes" : "No") << " | "
                      << (col.not_null ? "Yes" : "No") << " | "
                      << (col.unique ? "Yes" : "No") << std::endl;
        }
    }

    void handleInsert(const std::vector<std::string>& args) {
        if (args.size() < 2) {
            std::cout << "Usage: insert TABLE_NAME VAL1 VAL2 ..." << std::endl;
            return;
        }
        
        std::string table_name = args[0];
        localdb::Table* table = db.getTable(table_name);
        
        if (!table) {
            std::cout << "Table '" << table_name << "' does not exist" << std::endl;
            return;
        }
        
        const auto& columns = table->getColumns();
        
        if (args.size() - 1 != columns.size()) {
            std::cout << "Error: Expected " << columns.size() << " values, got " << (args.size() - 1) << std::endl;
            return;
        }
        
        localdb::Row row;
        
        // Parse values based on column types
        for (size_t i = 0; i < columns.size(); ++i) {
            try {
                row.push_back(parseValue(args[i + 1], columns[i].type));
            } catch (const std::exception& e) {
                std::cout << "Error parsing value for column '" << columns[i].name << "': " << e.what() << std::endl;
                return;
            }
        }
        
        // Insert the row
        bool success;
        if (current_transaction) {
            success = current_transaction->insert(table_name, row);
        } else {
            auto tx = db.beginTransaction();
            success = tx->insert(table_name, row);
            tx->commit();
        }
        
        if (success) {
            std::cout << "Row inserted successfully" << std::endl;
        } else {
            std::cout << "Failed to insert row" << std::endl;
        }
    }

    void handleSelect(const std::vector<std::string>& args) {
        if (args.empty()) {
            std::cout << "Usage: select TABLE_NAME [WHERE COL_INDEX OPERATOR VALUE]" << std::endl;
            return;
        }
        
        std::string table_name = args[0];
        localdb::Table* table = db.getTable(table_name);
        
        if (!table) {
            std::cout << "Table '" << table_name << "' does not exist" << std::endl;
            return;
        }
        
        const auto& columns = table->getColumns();
        
        // Parse where clause if present
        bool has_where = args.size() > 1 && args[1] == "WHERE";
        int col_index = -1;
        std::string op;
        std::string value_str;
        
        if (has_where && args.size() >= 5) {
            try {
                col_index = std::stoi(args[2]);
                op = args[3];
                value_str = args[4];
                
                if (col_index < 0 || col_index >= static_cast<int>(columns.size())) {
                    std::cout << "Invalid column index: " << col_index << std::endl;
                    return;
                }
            } catch (const std::exception& e) {
                std::cout << "Error parsing WHERE clause: " << e.what() << std::endl;
                return;
            }
        }
        
        // Create predicate function
        auto predicate = [&](const localdb::Row& row) -> bool {
            if (!has_where) return true;
            
            const auto& col_val = row[col_index];
            const auto& col_type = columns[col_index].type;
            
            try {
                if (op == "=") {
                    return col_val == parseValue(value_str, col_type);
                } else if (op == "<") {
                    return col_val < parseValue(value_str, col_type);
                } else if (op == ">") {
                    // Use < in reverse
                    return parseValue(value_str, col_type) < col_val;
                } else if (op == "!=") {
                    return col_val != parseValue(value_str, col_type);
                } else {
                    std::cout << "Unsupported operator: " << op << std::endl;
                    return false;
                }
            } catch (const std::exception& e) {
                std::cout << "Error evaluating predicate: " << e.what() << std::endl;
                return false;
            }
        };
        
        // Execute the query
        std::vector<localdb::Row> results;
        if (current_transaction) {
            results = current_transaction->select(table_name, predicate);
        } else {
            auto tx = db.beginTransaction();
            results = tx->select(table_name, predicate);
            tx->commit();
        }
        
        // Display results
        if (results.empty()) {
            std::cout << "No rows found" << std::endl;
            return;
        }
        
        // Display column headers
        displayHeader(columns);
        
        // Display rows
        for (const auto& row : results) {
            displayRow(row, columns);
        }
        
        std::cout << results.size() << " row(s) returned" << std::endl;
    }

    void handleUpdate(const std::vector<std::string>&) {
        std::cout << "Update operation not fully implemented yet." << std::endl;
        // TODO: Implement update
    }

    void handleDelete(const std::vector<std::string>& args) {
        if (args.size() < 4 || args[1] != "WHERE") {
            std::cout << "Usage: delete TABLE_NAME WHERE COL_INDEX OPERATOR VALUE" << std::endl;
            return;
        }
        
        std::string table_name = args[0];
        localdb::Table* table = db.getTable(table_name);
        
        if (!table) {
            std::cout << "Table '" << table_name << "' does not exist" << std::endl;
            return;
        }
        
        const auto& columns = table->getColumns();
        
        // Parse where clause
        int col_index;
        std::string op;
        std::string value_str;
        
        try {
            col_index = std::stoi(args[2]);
            op = args[3];
            value_str = args[4];
            
            if (col_index < 0 || col_index >= static_cast<int>(columns.size())) {
                std::cout << "Invalid column index: " << col_index << std::endl;
                return;
            }
        } catch (const std::exception& e) {
            std::cout << "Error parsing WHERE clause: " << e.what() << std::endl;
            return;
        }
        
        // Create predicate function
        auto predicate = [&](const localdb::Row& row) -> bool {
            const auto& col_val = row[col_index];
            const auto& col_type = columns[col_index].type;
            
            try {
                if (op == "=") {
                    return col_val == parseValue(value_str, col_type);
                } else if (op == "<") {
                    return col_val < parseValue(value_str, col_type);
                } else if (op == ">") {
                    return parseValue(value_str, col_type) < col_val;
                } else if (op == "!=") {
                    return col_val != parseValue(value_str, col_type);
                } else {
                    std::cout << "Unsupported operator: " << op << std::endl;
                    return false;
                }
            } catch (const std::exception& e) {
                std::cout << "Error evaluating predicate: " << e.what() << std::endl;
                return false;
            }
        };
        
        // Execute the delete
        bool success;
        if (current_transaction) {
            success = current_transaction->remove(table_name, predicate);
        } else {
            auto tx = db.beginTransaction();
            success = tx->remove(table_name, predicate);
            tx->commit();
        }
        
        if (success) {
            std::cout << "Row(s) deleted successfully" << std::endl;
        } else {
            std::cout << "Failed to delete row(s)" << std::endl;
        }
    }

    void handleBeginTransaction(const std::vector<std::string>&) {
        if (current_transaction) {
            std::cout << "Transaction already in progress. Commit or rollback first." << std::endl;
            return;
        }
        
        current_transaction = db.beginTransaction();
        std::cout << "Transaction started" << std::endl;
    }

    void handleCommitTransaction(const std::vector<std::string>&) {
        if (!current_transaction) {
            std::cout << "No transaction in progress" << std::endl;
            return;
        }
        
        bool success = current_transaction->commit();
        current_transaction = nullptr;
        
        if (success) {
            std::cout << "Transaction committed successfully" << std::endl;
        } else {
            std::cout << "Failed to commit transaction" << std::endl;
        }
    }

    void handleRollbackTransaction(const std::vector<std::string>&) {
        if (!current_transaction) {
            std::cout << "No transaction in progress" << std::endl;
            return;
        }
        
        current_transaction->rollback();
        current_transaction = nullptr;
        std::cout << "Transaction rolled back" << std::endl;
    }

    void handleSaveDatabase(const std::vector<std::string>& args) {
        if (args.empty()) {
            std::cout << "Usage: save FILENAME" << std::endl;
            return;
        }
        
        std::string filename = args[0];
        bool success = db.saveToFile(filename);
        
        if (success) {
            current_db_file = filename;
            std::cout << "Database saved to '" << filename << "'" << std::endl;
            
            // Show file size
            try {
                auto filesize = fs::file_size(filename);
                std::cout << "File size: " << filesize << " bytes" << std::endl;
            } catch (const fs::filesystem_error& e) {
                std::cerr << "Error getting file size: " << e.what() << std::endl;
            }
        } else {
            std::cout << "Failed to save database to '" << filename << "'" << std::endl;
        }
    }

    void handleLoadDatabase(const std::vector<std::string>& args) {
        if (args.empty()) {
            std::cout << "Usage: load FILENAME" << std::endl;
            return;
        }
        
        std::string filename = args[0];
        
        // Check if file exists
        if (!fs::exists(filename)) {
            std::cout << "File '" << filename << "' does not exist" << std::endl;
            return;
        }
        
        bool success = db.loadFromFile(filename);
        
        if (success) {
            current_db_file = filename;
            std::cout << "Database loaded from '" << filename << "'" << std::endl;
            
            // Display tables
            auto tables = db.getTableNames();
            std::cout << "Tables loaded: " << tables.size() << std::endl;
            for (const auto& table : tables) {
                std::cout << "  " << table << std::endl;
            }
        } else {
            std::cout << "Failed to load database from '" << filename << "'" << std::endl;
        }
    }
};

int main(int argc, char* argv[]) {
    LocalDBCLI cli;
    
    // Handle command-line arguments
    if (argc > 1) {
        std::string arg = argv[1];
        if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: localdb [--load FILENAME]" << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  --help, -h     Show this help message" << std::endl;
            std::cout << "  --load FILE    Load database from FILE" << std::endl;
            return 0;
        } else if (arg == "--load" && argc > 2) {
            std::vector<std::string> load_args = {argv[2]};
            cli.processCommand("load " + std::string(argv[2]));
        }
    }
    
    cli.run();
    return 0;
}
