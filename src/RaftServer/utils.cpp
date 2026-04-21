//
// Created by Desktop on 2026/1/15.
//

#include "utils.h"
#include <random>

int utils::getRandomNumber(int min, int max) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(min, max);
    return dis(gen);
}
