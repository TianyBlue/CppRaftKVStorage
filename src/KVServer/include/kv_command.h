//
// Created by Desktop on 2026/1/22.
//

#ifndef RAFTKVSTORAGE_KVCOMMAND_H
#define RAFTKVSTORAGE_KVCOMMAND_H

#include <string>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/serialization.hpp>

#include <cstdint>

#include "kv_server_proto_enum.h"

enum class CommandType {
    GET,
    PUT,
    DELETE,
};

struct KVCommand {
    CommandType type;
    std::string key;
    std::string val;

    std::int64_t clientId;
    std::int64_t requestId;

    KVExecuteState::KVExecuteState execStatus;

    std::string toString() const {
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);
        oa << *this;
        return ss.str();
    }

    static KVCommand fromString(const std::string& str) {
        KVCommand command;

        std::stringstream ss(str);
        boost::archive::text_iarchive ia(ss);
        ia >> command;
        return command;
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & type;
        ar & key;
        ar & val;
        ar & clientId;
        ar & requestId;
        ar & execStatus;
    }

    friend class boost::serialization::access;
};


#endif //RAFTKVSTORAGE_KVCOMMAND_H