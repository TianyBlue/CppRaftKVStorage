//
// Created by Desktop on 2026/1/15.
//

#ifndef RAFTKVSTORAGE_UTILS_H
#define RAFTKVSTORAGE_UTILS_H
#include <string>
#include <format>
#include <chrono>

namespace utils {
    int getRandomNumber(int min, int max);

    template <typename TimePoint>
    // requires requires(TimePoint){std::is_same_v<TimePoint, std::chrono::steady_clock::time_point>();}
    std::string getFormattedTime(const TimePoint &time) {
        using namespace std::chrono;
        auto since_epoch = time.time_since_epoch();
        auto secs = duration_cast<seconds>(since_epoch);
        auto ms = duration_cast<milliseconds>(since_epoch) % 1000;
        return std::format("{}.{:03}s", secs.count(), ms.count());
    }
}

#endif //RAFTKVSTORAGE_UTILS_H