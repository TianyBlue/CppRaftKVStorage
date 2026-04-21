//
// Created by Desktop on 2026/1/27.
//

#include "kv_server_node.h"

#include <bits/types/sigset_t.h>
#include <csignal>
#include <exception>
#include <iostream>
#include <string>
#include "config.h"

int main(int argc, char* argv[]) {
    std::string configPath;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg.substr(0, 8) == "-config=") {
            configPath = arg.substr(8);
        } else {
            std::cerr << "Error: Unknown argument '" << arg << "'\n";
            std::cerr << "Usage: " << argv[0] << " -config=<file>\n";
            return 1;
        }
    }

    if (configPath.empty()) {
        std::cerr << "Error: Missing required argument '-config=<file>'\n";
        std::cerr << "Usage: " << argv[0] << " -config=<file>\n";
        return 1;
    }

    std::cout << "Config file: " << configPath << "\n";

    // configPath = "./kv_config.yaml";

    try {
        std::string configError;
        auto cfgOpt = KVNodeConfig::parseFromFile(configPath, &configError);
        if (!cfgOpt.has_value()) {
            std::cerr << "Error: failed to start KVServerNode: " << configError << "\n";
            return 1;
        }

        KVServerNode node;
        node.init(std::move(cfgOpt.value()));
        // node.runAndWait();


        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGINT);
        sigaddset(&set, SIGTERM);
        pthread_sigmask(SIG_BLOCK, &set, nullptr);

        node.run();

        int sig = 0;
        sigwait(&set, &sig);

        node.stop();
    } catch (const std::exception& e) {
        std::cerr << "Error: failed to start KVServerNode: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
