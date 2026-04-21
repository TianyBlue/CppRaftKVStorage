//
// Created by Desktop on 2026/1/24.
//

#include <vector>
#include <string>
#include <iostream>
#include "kv_client.h"
#include <magic_enum/magic_enum.hpp>

std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;

    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }

    return tokens;
}

int main() {
    std::vector<int> clusterId{1, 2, 3};
    std::vector<std::string> clusterAddr{"127.0.0.1:50001", "127.0.0.1:50002", "127.0.0.1:50003"};


    KVClient client;
    client.init(clusterAddr);
    std::cout << "start client, please input command, press q to quit:\n";
    std::cout << "eg. put key val; get key; remove key;\n" << std::endl;
    while (true) {
        std::string command;
        std::getline(std::cin, command);

        auto tokens = split(command, ' ');
        if (tokens.empty() || tokens.size() > 3) {
            std::cout << "invalid command. try again\n";
            continue;
        }

        if (tokens[0] == "q")
            break;

        std::string op = tokens[0];
        if (op == "put") {
            if (tokens.size() != 3) {
                std::cout << "invalid command. try again\n";
                continue;
            }

            std::string key = tokens[1];
            std::string value = tokens[2];

            auto result = client.put(key, value);
            std::cout << "put result: " << magic_enum::enum_name(result) << std::endl;
        }
        else if (op == "get") {
            if (tokens.size() != 2) {
                std::cout << "invalid command. try again\n";
                continue;
            }

            std::string key = tokens[1];
            std::string value{};

            auto result = client.get(key, value);
            std::cout << std::format("get result: {} , value: {}", magic_enum::enum_name(result), value) << std::endl;
        }
        else if (op == "remove") {
            if (tokens.size() != 2) {
                std::cout << "invalid command. try again\n";
                continue;
            }

            std::string key = tokens[1];

            auto result = client.remove(key);
            std::cout << "remove result: " << magic_enum::enum_name(result) << std::endl;
        }
        else {
            std::cout << "invalid command. try again.\n";
        }
    }
    return 0;

}