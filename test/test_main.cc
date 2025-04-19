#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <iostream>

// 全局超时变量
std::atomic<bool> timeout_occurred(false);
const int GLOBAL_TEST_TIMEOUT_SEC = 30; // 30秒全局超时

// 显示超时警告而不是中断测试
void print_timeout_warning() {
    std::cerr << "\n\n*** WARNING: TEST SUITE RUNNING FOR MORE THAN "
              << GLOBAL_TEST_TIMEOUT_SEC << " SECONDS ***\n"
              << "This may indicate a deadlock or infinite loop.\n"
              << "Consider investigating or interrupting manually.\n\n";
}

class GlobalTimeoutListener : public ::testing::EmptyTestEventListener {
public:
    GlobalTimeoutListener() : start_time_(std::chrono::steady_clock::now()) {}
    
    virtual void OnTestProgramStart(const ::testing::UnitTest& unit_test) override {
        start_time_ = std::chrono::steady_clock::now();
        
        // 启动监控线程
        monitor_thread_ = std::thread([this]() {
            while (!done_) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                auto elapsed = std::chrono::steady_clock::now() - start_time_;
                if (elapsed > std::chrono::seconds(GLOBAL_TEST_TIMEOUT_SEC) && !warning_printed_) {
                    print_timeout_warning();
                    warning_printed_ = true;
                }
            }
        });
    }
    
    virtual void OnTestProgramEnd(const ::testing::UnitTest& unit_test) override {
        done_ = true;
        if (monitor_thread_.joinable()) {
            monitor_thread_.join();
        }
    }
    
private:
    std::chrono::steady_clock::time_point start_time_;
    std::thread monitor_thread_;
    std::atomic<bool> done_{false};
    std::atomic<bool> warning_printed_{false};
};

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    
    // 添加自定义监听器
    ::testing::TestEventListeners& listeners = 
        ::testing::UnitTest::GetInstance()->listeners();
    listeners.Append(new GlobalTimeoutListener());
    
    return RUN_ALL_TESTS();
} 