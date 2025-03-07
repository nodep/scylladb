#pragma once

#include <unordered_map>

namespace locator {

struct temporal_tablet_id {
    table_id table_id;
    dht::token last_token;

    bool operator == (const temporal_tablet_id&) const = default;
};
}

namespace std {
template <>
struct hash<locator::temporal_tablet_id> {
    size_t operator()(const locator::temporal_tablet_id& id) const {
        return utils::hash_combine(std::hash<table_id>()(id.table_id),
                                   std::hash<dht::token>()(id.last_token));
    }
};
}

namespace locator {

struct disk_usage {
    uint64_t used = 0;
    uint64_t capacity = 0; 
};

struct host_load_stats {
    std::unordered_map<shard_id, disk_usage> usage_by_shard;
    std::unordered_map<temporal_tablet_id, uint64_t> tablet_sizes;

    disk_usage get_sum() const;
};  
    
using cluster_disk_usage = std::unordered_map<host_id, host_load_stats>;

}

inline sstring size2gb(uint64_t size, int decimals = 2) {
    return std::format("{:.{}f} Gb", size / 1024.0 / 1024.0 / 1024.0, decimals);
}   
