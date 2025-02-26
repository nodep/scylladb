/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "locator/topology.hh"
#include "locator/token_metadata.hh"
#include "locator/tablets.hh"
#include "utils/stall_free.hh"
#include "utils/extremum_tracking.hh"
#include "utils/div_ceil.hh"

#include <optional>
#include <vector>

namespace locator {

constexpr double count_influence = 0;
constexpr size_t ideal_tablet_count = 100;
constexpr double balance_load_delta = .005;
constexpr uint64_t huge_tablet_size_threshold = 5UL * 1024 * 1024 * 1024 * 10;

inline sstring brief(host_id h) {
    return ::format("{:.2}", ::format("{}", h));
}

inline sstring brief(table_id t) {
    return ::format("{:.2}", ::format("{}", t));
}

inline sstring brief(global_tablet_id gt) {
    return ::format("{}:{}", brief(gt.table), gt.tablet);
}

using load_type = double;

load_type interpolate(load_type in);
load_type compute_load(const locator::disk_usage& disk_usage, size_t tablet_count, size_t n_shards);

class size_load_sketch {
    using shard_id = seastar::shard_id;

    struct shard_load {
        shard_id id = 0;
        load_type load = 0;

        locator::disk_usage du;
        size_t tablet_count = 0;

        void update() {
            load = compute_load(du, tablet_count, 1);
        }

        // Less-comparator which orders by load first (ascending), and then by shard id (ascending).
        bool operator < (const shard_load& rhs) const {
            return load == rhs.load ? id < rhs.id : load < rhs.load;
        };

    };

    struct node_load {
        std::vector<shard_load> _shards;
        std::vector<shard_id> _shards_by_load;

        load_type _load = 0;
        locator::disk_usage _du;
        size_t _tablet_count = 0;

        node_load(size_t shard_count, const locator::disk_usage& du)
            : _shards(shard_count)
            , _du(du)
        {
            for (shard_id sid = 0; sid < shard_count; sid++) {
                _shards_by_load.push_back(sid);
            }
        }

        void update_shard_load(shard_id shard, int tablet_delta, uint64_t size) {
            shard_load& shl = _shards[shard];

            if (tablet_delta < 0) {
                _tablet_count--;
                _du.used -= size;
                shl.tablet_count--;
                shl.du.used -= size;
            } else {
                _tablet_count++;
                _du.used += size;
                shl.tablet_count++;
                shl.du.used += size;
            }

            shl.update();
            update();
        }

        void populate_shards_by_load() {
            for (shard_id i = 0; i < _shards.size(); ++i) {
                _shards[i].update();
            }

            update();
        }

        load_type& load() noexcept {
            return _load;
        }

        const load_type& load() const noexcept {
            return _load;
        }

        void update() {
            _load = compute_load(_du, _tablet_count, _shards.size());
            std::sort(_shards_by_load.begin(), _shards_by_load.end(), [&, this]
                        (shard_id& lhs, shard_id& rhs) {
                            return _shards[lhs] < _shards[rhs];
                        });
        }
    };
    std::unordered_map<host_id, node_load> _nodes;
    token_metadata_ptr _tm;
    cluster_disk_usage* _cluster_du;
private:
    tablet_replica_set get_replicas_for_tablet_load(const tablet_info& ti, const tablet_transition_info* trinfo) const {
        // We reflect migrations in the load as if they already happened,
        // optimistically assuming that they will succeed.
        return trinfo ? trinfo->next : ti.replicas;
    }

    future<> populate_table(const tablet_map& tmap, const table_id& table, std::optional<host_id> host, std::optional<sstring> only_dc) {
        const topology& topo = _tm->get_topology();
        co_await tmap.for_each_tablet([&] (tablet_id tid, const tablet_info& ti) -> future<> {
            for (auto&& replica : get_replicas_for_tablet_load(ti, tmap.get_tablet_transition_info(tid))) {
                if (host && *host != replica.host) {
                    continue;
                }
                const host_load_stats& host_load = _cluster_du->at(replica.host);
                if (!_nodes.contains(replica.host)) {
                    auto node = topo.find_node(replica.host);
                    if (only_dc && node->dc_rack().dc != *only_dc) {
                        continue;
                    }
                    locator::disk_usage du = host_load.get_sum();
                    du.used = 0;
                    node_load nl{node->get_shard_count(), du};
                    for (shard_id sid = 0; sid < nl._shards.size(); sid++) {
                        nl._shards[sid].du.capacity = host_load.usage_by_shard.at(sid).capacity;
                    }
                    _nodes.emplace(replica.host, nl);
                }
                node_load& n = _nodes.at(replica.host);
                // find the tablet
                dht::token last_token = tmap.get_last_token(tid);
                temporal_tablet_id ttablet_id {table, last_token};
                auto tsi = host_load.tablet_sizes.find(ttablet_id);
                if (tsi == host_load.tablet_sizes.end()) {
                    throw std::runtime_error(::format("table {} last_token {} not found", table, last_token));
                }
                const uint64_t tablet_size = tsi->second;
                if (replica.shard < n._shards.size()) {
                    n._tablet_count += 1;
                    n._shards[replica.shard].tablet_count += 1;
                    n._shards[replica.shard].du.used += tablet_size;
                }
                n._du.used += tablet_size;
        }
            return make_ready_future<>();
        });
    }
public:
    size_load_sketch(token_metadata_ptr tm, cluster_disk_usage& cluster_du)
        : _tm(std::move(tm))
        , _cluster_du(&cluster_du) {
    }

    future<> populate(std::optional<host_id> host = std::nullopt,
                      std::optional<table_id> only_table = std::nullopt,
                      std::optional<sstring> only_dc = std::nullopt) {
        co_await utils::clear_gently(_nodes);

        if (only_table) {
            auto& tmap = _tm->tablets().get_tablet_map(*only_table);
            co_await populate_table(tmap, *only_table, host, only_dc);
        } else {
            for (auto&& [table, tmap]: _tm->tablets().all_tables()) {
                co_await populate_table(*tmap, table, host, only_dc);
            }
        }

        for (auto&& [id, n] : _nodes) {
            n.populate_shards_by_load();
        }
    }

    future<> populate_dc(const sstring& dc) {
        return populate(std::nullopt, std::nullopt, dc);
    }

    node_load& ensure_node(host_id node) {
        if (!_nodes.contains(node)) {
            const topology& topo = _tm->get_topology();
            auto shard_count = topo.find_node(node)->get_shard_count();
            if (shard_count == 0) {
                throw std::runtime_error(format("Shard count not known for node {}", node));
            }
            locator::disk_usage du = _cluster_du->at(node).get_sum();
            du.used = 0;
            auto [i, _] = _nodes.emplace(node, node_load{shard_count, du});
            i->second.populate_shards_by_load();
        }
        return _nodes.at(node);
    }

    shard_id get_least_loaded_shard(host_id node) {
        node_load& nl = ensure_node(node);
        return nl._shards_by_load.front();
    }

    shard_id get_most_loaded_shard(host_id node) {
        node_load& nl = ensure_node(node);
        return nl._shards_by_load.back();
    }

    void unload(host_id node, shard_id shard, uint64_t size) {
        auto& n = _nodes.at(node);
        n.update_shard_load(shard, -1, size);
    }

    void pick(host_id node, shard_id shard, uint64_t size) {
        auto& n = _nodes.at(node);
        n.update_shard_load(shard, 1, size);
    }

    const disk_usage& get_disk_usage(host_id node) {
        node_load& nl = ensure_node(node);
        return nl._du;
    } 
    
    load_type get_load(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        return _nodes.at(node).load();
    }

    load_type total_load() const {
        load_type total = 0;
        for (auto&& n : _nodes) {
            total += n.second.load();
        }
        return total;
    }

    load_type get_avg_shard_load(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        auto& n = _nodes.at(node);
        return n.load() / n._shards.size();
    }

    double get_real_avg_shard_load(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        auto& n = _nodes.at(node);
        return double(n.load()) / n._shards.size();
    }

    shard_id get_shard_count(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        return _nodes.at(node)._shards.size();
    }

    // Returns the difference in tablet count between highest-loaded shard and lowest-loaded shard.
    // Returns 0 when shards are perfectly balanced.
    // Returns 1 when shards are imbalanced, but it's not possible to balance them.
    load_type get_shard_imbalance(host_id node) const {
        auto minmax = get_shard_minmax(node);
        return minmax.max() - minmax.max();
    }

    min_max_tracker<load_type> get_shard_minmax(host_id node) const {
        min_max_tracker<load_type> minmax;
        if (_nodes.contains(node)) {
            auto& n = _nodes.at(node);
            for (auto&& load: n._shards) {
                minmax.update(load.load);
            }
        } else {
            minmax.update(0);
        }
        return minmax;
    }

    void dump();

    locator::disk_usage get_disk_usage(host_id node) const {
        if (!_nodes.contains(node)) {
            return {};
        }
        return _nodes.at(node)._du;
    }
};

} // namespace locator
