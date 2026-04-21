//
// Created by Desktop on 2026/1/21.
//

#ifndef RAFTKVSTORAGE_BLOCKING_QUEUE_H
#define RAFTKVSTORAGE_BLOCKING_QUEUE_H
#include <mutex>
#include <queue>
#include <condition_variable>

enum class PopResult {
    Timeout,
    Closed,
    Success
};

/**
 * ?锁阻塞队列, 类似于Golang的chan
 * @tparam T
 */
template <typename T>
class BlockingQueue {
public:
    explicit BlockingQueue(int queueSize)
        : m_MaxQueueSize(queueSize)
    {
        if (queueSize <= 0) {
            throw std::invalid_argument("queue size must > 0");
        }
    }

    BlockingQueue(const BlockingQueue& other) = delete;
    BlockingQueue(BlockingQueue&& other) noexcept = default;
    BlockingQueue& operator=(BlockingQueue&& rhs) noexcept = default;

    /**
     * ?队列入队, 如果队列已满则阻塞
     * @param item
     */
    void push(const T& item);
    void push(T&& item);

    PopResult pop(T& item);
    PopResult popWithTimeout(T& item, std::chrono::milliseconds timeout);

    void close();
    bool isClosed() const;
    size_t size() const;

private:
    mutable std::mutex m_Mutex;
    std::queue<T> m_Queue;
    std::condition_variable m_Cond;
    bool m_Closed{};
    int m_MaxQueueSize;
};

template<typename T>
void BlockingQueue<T>::push(const T &item) {
    {
        std::unique_lock lk{m_Mutex};
        m_Cond.wait(lk, [this]() {
            return m_Closed || m_Queue.size() < m_MaxQueueSize;
        });

        if (m_Closed) {
            return;
        }
        m_Queue.push(item);
    }
    m_Cond.notify_one();
}

template<typename T>
void BlockingQueue<T>::push(T &&item) {
    {
        std::unique_lock lk{m_Mutex};
        m_Cond.wait(lk, [this]() {
            return m_Closed || m_Queue.size() < m_MaxQueueSize;
        });

        if (m_Closed) {
            return;
        }
        m_Queue.push(std::forward<T>(item));
    }

    m_Cond.notify_one();
}

template<typename T>
PopResult BlockingQueue<T>::pop(T &item) {
    std::unique_lock lk{m_Mutex};
    m_Cond.wait(lk, [this]() {
        return m_Closed || !m_Queue.empty();
    });

    // close且队列为空，不继续等待
    if (m_Closed && m_Queue.empty())
        return PopResult::Closed;

    item = std::move(m_Queue.front());
    m_Queue.pop();
    m_Cond.notify_one();
    return PopResult::Success;
}

template<typename T>
PopResult BlockingQueue<T>::popWithTimeout(T &item, std::chrono::milliseconds timeout) {
    std::unique_lock lk{m_Mutex};
    m_Cond.wait_for(lk, timeout, [this]() {
        return m_Closed || !m_Queue.empty();
    });

    if (!m_Closed && m_Queue.empty())
        return PopResult::Timeout;
    else if (m_Closed) {
        return PopResult::Closed;
    }

    item = std::move(m_Queue.front());
    m_Queue.pop();
    m_Cond.notify_one();
    return PopResult::Success;
}

template<typename T>
void BlockingQueue<T>::close() {
    std::lock_guard lk{m_Mutex};
    m_Closed = true;
    m_Cond.notify_all();
    return;
}

template<typename T>
bool BlockingQueue<T>::isClosed() const {
    std::lock_guard lk{m_Mutex};
    return m_Closed;
}

template<typename T>
size_t BlockingQueue<T>::size() const {
    std::lock_guard lk{m_Mutex};
    return m_Queue.size();
}


#endif //RAFTKVSTORAGE_BLOCKING_QUEUE_H
