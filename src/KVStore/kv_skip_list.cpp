//
// Created by Desktop on 2026/1/23.
//

#include "include/kv_skip_list.h"

#include <boost/archive/basic_text_iarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/vector.hpp>
#include <string>
#include <utility>
#include "skip_list.h"

KVSkipList::KVSkipList() {
}

std::optional<std::string> KVSkipList::get(const std::string &key) {
    std::string val;
    if (m_SkipList.get(key, val)) {
        return val;
    }
    return std::nullopt;
}

void KVSkipList::put(const std::string &key, const std::string &value) {
    m_SkipList.put(key, value);
}

bool KVSkipList::erase(const std::string &key) {
    return m_SkipList.erase(key);
}
std::string KVSkipList::makeSnapshot() {
    std::vector<std::pair<std::string, std::string>> snapshotData{};
    snapshotData.reserve(m_SkipList.size());

    auto it = m_SkipList.iterator();
    while (it.hasNext()) {
        // auto [key, value] = it.next();
        snapshotData.push_back( it.next());
    }

    std::ostringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << snapshotData;

    return ss.str();
}

void KVSkipList::loadSnapshot(const std::string &snapshot) {
    if (snapshot.empty())
        return;

    std::vector<std::pair<std::string, std::string>> snapshotData{};
    std::stringstream ss(snapshot);
    boost::archive::text_iarchive ia(ss);
    ia >> snapshotData;

    // 清空SkipList
    m_SkipList = SkipList<std::string, std::string>{};
    for (auto& [key, value] : snapshotData) {
        m_SkipList.put(std::move(key), std::move(value));
    }
}

void KVSkipList::loadData(const std::vector<std::pair<std::string, std::string>>& data){
    m_SkipList = SkipList<std::string, std::string>();

    for(auto& [k, v] : data){
        m_SkipList.put(k, v);
    }
}

std::vector<std::pair<std::string, std::string>> KVSkipList::dumpData() const{
    std::vector<std::pair<std::string, std::string>> data;
    data.reserve(m_SkipList.size());

    auto it = m_SkipList.iterator();
    while (it.hasNext()) {
        data.push_back( it.next());
    }

    return data;
}
