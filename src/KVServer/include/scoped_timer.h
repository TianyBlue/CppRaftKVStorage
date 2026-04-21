#ifndef SCOPED_TIMER
#define SCOPED_TIMER

#include <chrono>
#include <functional>

template <typename Callable> requires requires(Callable _callable){
    _callable(std::chrono::milliseconds{});
}
class ScopedTimer {
public:
    ScopedTimer(Callable&& finishFn) : m_FinishFn(std::forward<Callable>(finishFn)) {
        m_Start = std::chrono::steady_clock::now();
    }

    ~ScopedTimer() {
        auto finishTime = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(finishTime - m_Start);
        m_FinishFn(duration);
    }

private:
    std::chrono::steady_clock::time_point m_Start{};
    Callable m_FinishFn{};
};


#endif