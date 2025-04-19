#ifndef LOCALDB_TEST_UTILS_H
#define LOCALDB_TEST_UTILS_H

#include <gtest/gtest.h>
#include <chrono>
#include <functional>
#include <thread>
#include <mutex>
#include <iostream>

namespace localdb_test {

// 测试超时异常类
class TestTimeoutException : public std::runtime_error {
public:
    TestTimeoutException(const std::string& what_arg) : std::runtime_error(what_arg) {}
};

// 超时保护函数，确保测试函数在指定时间内完成
template<typename Func>
void runWithTimeout(Func testFunc, std::chrono::milliseconds timeout, const std::string& testName) {
    bool completed = false;
    std::exception_ptr exception;
    
    std::thread testThread([&]() {
        try {
            testFunc();
            completed = true;
        } catch (const std::exception& e) {
            exception = std::current_exception();
        }
    });
    
    if (testThread.joinable()) {
        auto status = std::cv_status::no_timeout;
        
        {
            std::mutex mtx;
            std::unique_lock<std::mutex> lock(mtx);
            std::condition_variable cv;
            
            status = cv.wait_for(lock, timeout, [&completed]() { 
                return completed; 
            });
        }
        
        if (status == std::cv_status::timeout) {
            std::cerr << "\nTEST TIMEOUT: " << testName << " has exceeded the timeout of " 
                      << timeout.count() << " ms\n";
            
            // 尝试让测试线程结束，这可能不会立即生效
            // 并且可能无法解决真正的死锁问题
            testThread.detach();
            
            throw TestTimeoutException("Test timeout: " + testName);
        } else {
            testThread.join();
            
            if (exception) {
                std::rethrow_exception(exception);
            }
        }
    }
}

// 测试安全锁定/解锁助手类
class TestSafeLock {
public:
    template<typename Mutex>
    static bool safeLock(Mutex& mutex, std::chrono::milliseconds timeout) {
        auto end_time = std::chrono::steady_clock::now() + timeout;
        
        while (std::chrono::steady_clock::now() < end_time) {
            if (mutex.try_lock()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        
        return false;
    }
    
    template<typename Mutex>
    static bool safeSharedLock(Mutex& mutex, std::chrono::milliseconds timeout) {
        auto end_time = std::chrono::steady_clock::now() + timeout;
        
        while (std::chrono::steady_clock::now() < end_time) {
            if (mutex.try_lock_shared()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        
        return false;
    }
};

} // namespace localdb_test

#endif // LOCALDB_TEST_UTILS_H 