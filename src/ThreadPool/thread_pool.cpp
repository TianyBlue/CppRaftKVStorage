//
// Created by Skylake on 2026/2/4.
//

#include "include/thread_pool.h"


class ThreadPool::Worker {
public:
    explicit Worker(ThreadPool* tp)
        :pool{tp}
    {
        t = std::thread(&Worker::loop, this);
    }

    ~Worker() {
        if(t.joinable()){
            t.join();
        }
    }

    std::thread t;
    ThreadPool* pool;

private:
    void loop() {
        while (true) {
            std::function<void()> taskToRun;
            {
                std::unique_lock lk{pool->m_Mutex};
                pool->m_Cond.wait(lk, [this]() {
                   return this->pool->m_Stop || !this->pool->m_Tasks.empty();
                });

                if (pool->m_Stop && pool->m_Tasks.empty()) {
                    return;
                }

                if (!pool->m_Tasks.empty()) {
                    taskToRun = std::move(pool->m_Tasks.front());
                    this->pool->m_Tasks.pop();
                }
            }

            taskToRun();
        }
    }
};

ThreadPool::ThreadPool(int thread_num)
    : m_MaxThreads{thread_num}
{
    m_Workers.reserve(m_MaxThreads);
    for (int i{}; i < m_MaxThreads; i++) {
        m_Workers.emplace_back(std::make_unique<Worker>(this));
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lk{m_Mutex};
        m_Stop.store(true);
        m_Cond.notify_all();
    }

    for (auto& worker : m_Workers) {
        if (worker->t.joinable()) {
            worker->t.join();
        }
    }
}
