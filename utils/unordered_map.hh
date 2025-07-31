/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <unordered_map>

#include <fmt/format.h>

#include <seastar/util/backtrace.hh>

namespace utils {

template <typename Key, typename T,
        class Hash = std::hash<Key>,
        class KeyEqual = std::equal_to<Key>,
        class Allocator = std::allocator<std::pair<const Key, T>>>
class unordered_map : public std::unordered_map<Key, T, Hash, KeyEqual, Allocator> {
public:
    using map_type = std::unordered_map<Key, T, Hash, KeyEqual, Allocator>;

    using map_type::map_type; // Inherit constructors

    unordered_map(const map_type& other) : map_type(other) {}
    unordered_map(map_type&& other) : map_type(std::move(other)) {}
    unordered_map& operator=(const map_type& other) {
        return static_cast<map_type&>(*this) = other;
    }
    unordered_map& operator=(map_type&& other) {
        return static_cast<map_type&>(*this) = std::move(other);
    }

    const T& at(const Key& key) const {
        auto it = this->find(key);
        if (it == this->end()) {
            throw std::out_of_range(fmt::format("Key '{}' not found in unordered_map, at {}", key, seastar::current_backtrace()));
        }
        return it->second;
    }

    T& at(const Key& key) {
        auto it = this->find(key);
        if (it == this->end()) {
            throw std::out_of_range(fmt::format("Key '{}' not found in unordered_map, at {}", key, seastar::current_backtrace()));
        }
        return it->second;
    }
};

} // namespace utils
