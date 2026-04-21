//
// Created by Skylake on 2026/2/15.
//

#ifndef RAFTKVSTORAGE_DEFER_CLASS_H
#define RAFTKVSTORAGE_DEFER_CLASS_H

#include <functional>

class DeferClass {

public:
    template<typename Callable>
    DeferClass(Callable _call)
        : m_DeferToRun(_call)
    {}

    ~DeferClass() {
        m_DeferToRun();
    }

private:
    std::function<void()> m_DeferToRun;
};


#endif // RAFTKVSTORAGE_DEFER_CLASS_H
