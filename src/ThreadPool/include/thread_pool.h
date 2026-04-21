//
// Created by Skylake on 2026/2/4.
//

#ifndef RAFTKVSTORAGE_THREAD_POOL_H
#define RAFTKVSTORAGE_THREAD_POOL_H

#include <future>
#include <queue>
#include <functional>
#include <memory>
#include <condition_variable>

class ThreadPool {
public:
    explicit ThreadPool(int thread_num);

    ~ThreadPool();

    template <class F, class... Args>
    auto enqueue(F&& f, Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type>;

private:
    class Worker;
    const int m_MaxThreads;

    std::queue<std::function<void()>> m_Tasks;
    std::mutex m_Mutex;
    std::condition_variable m_Cond;
    std::atomic<bool> m_Stop{false};
    // 首先析构
    std::vector<std::unique_ptr<Worker>> m_Workers;
};

template<class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&...args) -> std::future<typename std::result_of<F(Args...)>::type> {
    using result_type = std::result_of_t<F(Args...)>;

    std::shared_ptr<std::packaged_task<result_type()>> task =
        std::make_shared<std::packaged_task<result_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    {
        std::lock_guard lk{m_Mutex};
        if (m_Stop) {
            throw std::runtime_error("enqueue on stopped ThreadPool");
        }

        m_Tasks.push([task](){(*task)();});
    }

    m_Cond.notify_one();
    return task->get_future();
}


#endif // RAFTKVSTORAGE_THREAD_POOL_H
