/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <fmt/ranges.h>
#include <bit>

#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include "locator/tablets.hh"
#include "service/tablet_allocator.hh"
#include "locator/tablet_replication_strategy.hh"
#include "locator/network_topology_strategy.hh"
#include "locator/size_load_sketch.hh"
#include "locator/load_sketch.hh"
#include "replica/tablets.hh"
#include "locator/tablet_replication_strategy.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"

#include "test/perf/perf.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/key_utils.hh"

using namespace locator;
using namespace replica;
using namespace service;

static seastar::abort_source aborted;

static const sstring dc = "dc1";

seastar::logger testlblog("testlblog");

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    return c;
}

static
future<table_id> add_table(cql_test_env& e) {
    static char cnt = 10;
    auto id = table_id(utils::UUID(::format("{:x}0000000-0000-0000-0000-000000000000", cnt++)));
    co_await e.create_table([id] (std::string_view ks_name) {
        return *schema_builder(ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                .build();
    });
    co_return id;
}

static
size_t get_tablet_count(const tablet_metadata& tm) {
    size_t count = 0;
    for (auto& [table, tmap] : tm.all_tables()) {
        count += std::accumulate(tmap->tablets().begin(), tmap->tablets().end(), size_t(0),
                                 [] (size_t accumulator, const locator::tablet_info& info) {
                                     return accumulator + info.replicas.size();
                                 });
    }
    return count;
}

static
void apply_resize_plan(token_metadata& tm, const migration_plan& plan) {
    for (auto [table_id, resize_decision] : plan.resize_plan().resize) {
        tm.tablets().mutate_tablet_map(table_id, [&resize_decision] (tablet_map& tmap) {
            resize_decision.sequence_number = tmap.resize_decision().sequence_number + 1;
            tmap.set_resize_decision(resize_decision);
        });
    }
    for (auto table_id : plan.resize_plan().finalize_resize) {
        auto& old_tmap = tm.tablets().get_tablet_map(table_id);
        testlblog.info("Setting new tablet map of size {}", old_tmap.tablet_count() * 2);
        tablet_map tmap(old_tmap.tablet_count() * 2);
        tm.tablets().set_tablet_map(table_id, std::move(tmap));
    }
}

// Reflects the plan in a given token metadata as if the migrations were fully executed.
static
void apply_plan(token_metadata& tm, const migration_plan& plan) {
    for (auto&& mig : plan.migrations()) {
        tm.tablets().mutate_tablet_map(mig.tablet.table, [&mig] (tablet_map& tmap) {
            auto tinfo = tmap.get_tablet_info(mig.tablet.tablet);
            tinfo.replicas = replace_replica(tinfo.replicas, mig.src, mig.dst);
            tmap.set_tablet(mig.tablet.tablet, tinfo);
        });
    }
    apply_resize_plan(tm, plan);
}

using seconds_double = std::chrono::duration<double>;

struct rebalance_stats {
    seconds_double elapsed_time = seconds_double(0);
    seconds_double max_rebalance_time = seconds_double(0);
    uint64_t rebalance_count = 0;
    uint64_t migrated_gb = 0;
    uint64_t migrated_tablets = 0;

    rebalance_stats& operator+=(const rebalance_stats& other) {
        elapsed_time += other.elapsed_time;
        max_rebalance_time = std::max(max_rebalance_time, other.max_rebalance_time);
        rebalance_count += other.rebalance_count;
        migrated_gb += other.migrated_gb;
        migrated_tablets += other.migrated_tablets;
        return *this;
    }
};

uint64_t get_shard_capacity(host_id h) {
    const std::vector<uint64_t> capacities {
        450UL * 1024 * 1024 * 1024,
        850UL * 1024 * 1024 * 1024,
        450UL * 1024 * 1024 * 1024,
        650UL * 1024 * 1024 * 1024,
    };

    const uint64_t i = abs(h.id.get_most_significant_bits() >> 56);
    const uint64_t capacity = capacities[i % capacities.size()];

    return capacity;
}

locator::cluster_disk_usage get_cluster_with_capacities(token_metadata_ptr tm) {
    locator::cluster_disk_usage disk_usage;

    // add all the nodes with their capacities
    tm->get_topology().for_each_node([&] (const locator::node& node) {
        host_id h = node.host_id();
        host_load_stats& hls = disk_usage[h];
        const uint64_t shard_capacity = get_shard_capacity(h);
        for (shard_id sid = 0; sid < node.get_shard_count(); sid++) {
            hls.usage_by_shard[sid].capacity = shard_capacity;
        }
    });

    return disk_usage;
}

locator::cluster_disk_usage calc_disk_usage(token_metadata_ptr tm, const std::unordered_map<global_tablet_id, uint64_t>& tablet_sizes) {

    // fill the disk capacities for nodes and shards
    locator::cluster_disk_usage disk_usage = get_cluster_with_capacities(tm);

    // recalc the disk usage and set the tablet sizes
    for (auto&& [table, tmap] : tm->tablets().all_tables()) {
        tmap->for_each_tablet([&] (tablet_id tid, const tablet_info& tinfo) -> future<> {
            const uint64_t size = tablet_sizes.at({table, tid});
            for (const tablet_replica& replica: tinfo.replicas) {
                host_load_stats& host_load = disk_usage.at(replica.host);
                host_load.usage_by_shard[replica.shard].used += size;
                dht::token last_token = tmap->get_last_token(tid);
                temporal_tablet_id ttablet_id {table, last_token};
                host_load.tablet_sizes[ttablet_id] = size;

                //auto h = ::format("{}", replica.host);
                //auto t = ::format("{}", table);
                //dbglog("{:.2} {:.2}:{} {}", h, t, tid, size2gb(size));
            }
            return make_ready_future<>();
        }).get();
    }

    return disk_usage;
}

locator::disk_usage host_load_stats::get_sum() const {
    disk_usage result;
    for (const disk_usage& du: usage_by_shard | std::views::values) {
        result.used += du.used;
        result.capacity += du.capacity;
    }
    return result;
}

sstring size2gb(uint64_t size, int decimals) {
    return std::format("{:.{}f} Gb", size / 1024.0 / 1024.0 / 1024.0, decimals);
}

sstring pprint(const locator::disk_usage& du) {
    return ::format("{} {} {:.2f}%", size2gb(du.used, 0), size2gb(du.capacity, 0), (du.used * 100.0) / du.capacity);
}

load_type locator::interpolate(load_type in) {
    struct point {
        double in = 0;
        double out = 0;
    };

    const std::vector<point> points = {
        /*
        {0,    0},
        {0.7,  0.7},
        {0.75, 0.8},
        {0.80, 0.95},
        {0.85, 1.2},
        {0.90, 1.8},
        {0.95, 3},
        {1.00, 5},
        */
        {0, 0},
        {1, 1},
    };

    size_t idx = 1;
    while ((idx < points.size() - 1) && (points[idx].in < in)) {
        idx++;
    }

    const point& p = points[idx];
    const point& last = points[idx - 1];

    return last.out + (in - last.in) * (p.out - last.out)/(p.in - last.in);
}

load_type locator::compute_load(const disk_usage& disk_usage, size_t tablet_count, size_t n_shards) {
    load_type disk_used = load_type(disk_usage.used) / disk_usage.capacity;
    load_type count_load = tablet_count / load_type(ideal_tablet_count * n_shards);
    load_type size_load = interpolate(disk_used);

    return size_load * (1 - count_influence) + count_load * count_influence;
}

void check_usage(const cluster_disk_usage& disk_usage) {
    for (const auto& [host_id, hls]: disk_usage) {
        const locator::disk_usage du = hls.get_sum();
        if (du.used > du.capacity) {
            throw std::runtime_error(::format("Node {:.2} is overloaded; used {} capacity {}",
                    ::format("{}", host_id), size2gb(du.used), size2gb(du.capacity)));
        }
    }
}

void size_load_sketch::dump()
{
    testlblog.info("****** dumping sketch");
    for (const auto& [host, node]: _nodes) {
        testlblog.info("*** host {} tablets {} shards {} du {} load {:.3f}",
                host, node._tablet_count, node._shards.size(), pprint(node._du), node._load);
        for (size_t i = 0; i < node._shards_by_load.size(); i++) {
            const shard_load& sl = node._shards[node._shards_by_load[i]];
            testlblog.info("   shard {} count {} size {} load {:.3f}", node._shards_by_load[i], sl.tablet_count, size2gb(sl.du.used), sl.load);
        }
    }
}

static
rebalance_stats rebalance_tablets(tablet_allocator& talloc, shared_token_metadata& stm, const std::unordered_map<global_tablet_id, uint64_t>& tablet_sizes,
                                    locator::load_stats_ptr load_stats = {}, std::unordered_set<host_id> skiplist = {}) {
    rebalance_stats stats;

    // Sanity limit to avoid infinite loops.
    // The x10 factor is arbitrary, it's there to account for more complex schedules than direct migration.
    auto max_iterations = 1 + get_tablet_count(stm.get()->tablets()) * 10;

    /*
    auto log_state = [&] (sstring header, const locator::cluster_disk_usage& disk_usage) {
        testlblog.info("{}", header);
        uint64_t umin = std::numeric_limits<uint64_t>::max();
        uint64_t umax = std::numeric_limits<uint64_t>::min();
        for (auto [host, host_usage]: disk_usage) {
            testlblog.info("host: {}", host);
            for (auto [shard, shard_usage]: host_usage.usage_by_shard) {
                umin = std::min(shard_usage.used, umin);
                umax = std::max(shard_usage.used, umax);
                testlblog.info("    shard: {} usage: {} capacity: {}", shard, size2gb(shard_usage.used), size2gb(shard_usage.capacity));
            }
        }

        uint64_t tmin = std::numeric_limits<uint64_t>::max();
        uint64_t tmax = std::numeric_limits<uint64_t>::min();
        for (auto&& [table, tmap] : stm.get()->tablets().all_tables()) {
            tmap->for_each_tablet([&] (tablet_id tid, const tablet_info& tinfo) -> future<> {
                const uint64_t size = tablet_sizes.at({table, tid});
                tmin = std::min(size, tmin);
                tmax = std::max(size, tmax);
                return make_ready_future<>();
            }).get();
        }
        testlblog.info("shard min/max: {}/{}", size2gb(umin), size2gb(umax));
        testlblog.info("tablet min/max: {}/{}", size2gb(tmin), size2gb(tmax));
    };
    */

    for (size_t i = 0; i < max_iterations; ++i) {
        auto prev_lb_stats = talloc.stats().for_dc(dc);

        locator::cluster_disk_usage disk_usage = calc_disk_usage(stm.get(), tablet_sizes);
        check_usage(disk_usage);

        auto start_time = std::chrono::steady_clock::now();
        auto plan = talloc.balance_tablets(stm.get(), load_stats, skiplist, disk_usage).get();
        auto end_time = std::chrono::steady_clock::now();
        auto lb_stats = talloc.stats().for_dc(dc) - prev_lb_stats;

        uint64_t migrated_total = 0;
        for (const tablet_migration_info& tmi : plan.migrations()) {
            migrated_total += tablet_sizes.at(tmi.tablet);
        }
        auto elapsed = std::chrono::duration_cast<seconds_double>(end_time - start_time);
        rebalance_stats iteration_stats = {
            .elapsed_time = elapsed,
            .max_rebalance_time = elapsed,
            .rebalance_count = 1,
            .migrated_gb = migrated_total,
            .migrated_tablets = plan.size(),
        };
        stats += iteration_stats;
        testlblog.debug("Rebalance iteration {} took {:.3f} [s]: mig={}, bad={}, first_bad={}, eval={}, skiplist={}, skip: (load={}, rack={}, node={})",
                      i + 1, elapsed.count(),
                      lb_stats.migrations_produced,
                      lb_stats.bad_migrations,
                      lb_stats.bad_first_candidates,
                      lb_stats.candidates_evaluated,
                      lb_stats.migrations_from_skiplist,
                      lb_stats.migrations_skipped,
                      lb_stats.tablets_skipped_rack,
                      lb_stats.tablets_skipped_node);

        if (plan.empty()) {
            testlblog.info("Rebalance took {:.3f} [s] after {} iteration(s), tablets moved: {} {}",
                            stats.elapsed_time.count(), i + 1, stats.migrated_tablets, size2gb(stats.migrated_gb));
            //log_state("******************** after", calc_disk_usage(stm.get(), tablet_sizes));
            return stats;
        }
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            apply_plan(tm, plan);
            return make_ready_future<>();
        }).get();
    }
    throw std::runtime_error("rebalance_tablets(): convergence not reached within limit");
}

struct params {
    int iterations;
    int nodes;
    std::optional<int> tablets1;
    std::optional<int> tablets2;
    int rf1;
    int rf2;
    int shards;
    int scale1 = 1;
    int scale2 = 1;
    std::optional<unsigned> seed;
};

struct table_balance {
    double shard_overcommit;
    double best_shard_overcommit;
    double node_overcommit;
};

constexpr auto nr_tables = 2;

struct cluster_balance {
    table_balance tables[nr_tables];
};

struct results {
    cluster_balance init;
    cluster_balance worst;
    cluster_balance last;
    rebalance_stats stats;
};

template<>
struct fmt::formatter<table_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const table_balance& b, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{shard={:.2f} (best={:.2f}), node={:.2f}}}",
                              b.shard_overcommit, b.best_shard_overcommit, b.node_overcommit);
    }
};

template<>
struct fmt::formatter<cluster_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const cluster_balance& r, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{table1={}, table2={}}}", r.tables[0], r.tables[1]);
    }
};

template<>
struct fmt::formatter<params> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const params& p, FormatContext& ctx) const {
        auto tablets1_per_shard = double(p.tablets1.value_or(0)) * p.rf1 / (p.nodes * p.shards);
        auto tablets2_per_shard = double(p.tablets2.value_or(0)) * p.rf2 / (p.nodes * p.shards);
        return fmt::format_to(ctx.out(), "{{iterations={}, nodes={}, tablets1={} ({:0.1f}/sh), tablets2={} ({:0.1f}/sh), rf1={}, rf2={}, shards={}}}",
                         p.iterations, p.nodes,
                         p.tablets1.value_or(0), tablets1_per_shard,
                         p.tablets2.value_or(0), tablets2_per_shard,
                         p.rf1, p.rf2, p.shards);
    }
};

struct tablet_size_generator {
    size_t _tablet_count = 0;

    tablet_size_generator(std::optional<unsigned> seed = std::nullopt) {
        const size_t used_seed = seed.value_or(time(NULL));
        testlblog.info("random seed: {}", used_seed);
        srand(used_seed);
    }

    uint64_t get(uint64_t range_min, uint64_t range_max) {
        const double range = range_max - range_min;
        const double factor = range / RAND_MAX;
        return uint64_t(range_min + rand() * factor);
    }

    uint64_t generate() {
        ++_tablet_count;
        if (_tablet_count % 1000 == 0) {
            return get(default_target_tablet_size * 60, default_target_tablet_size * 70);
        }
        return get(0, default_target_tablet_size * 2);
    }
};

/*
struct tablet_size_generator {
    std::random_device  _rd;
    std::mt19937        _gen;
    std::piecewise_linear_distribution<> _d;

    tablet_size_generator()
        : _gen(_rd())
    {
        std::vector<double> i, w;
        i.push_back(0);                                 w.push_back(0);
        i.push_back(default_target_tablet_size * .6);   w.push_back(80);
        i.push_back(default_target_tablet_size * .8);   w.push_back(85);
        i.push_back(default_target_tablet_size);        w.push_back(80);
        i.push_back(default_target_tablet_size * 1.2);  w.push_back(60);
        i.push_back(default_target_tablet_size * 2);    w.push_back(0.1);
        i.push_back(default_target_tablet_size * 100);  w.push_back(0);

        _d = std::piecewise_linear_distribution<>(i.begin(), i.end(), w.begin());
    }

    uint64_t generate() {
        return _d(_gen) / 2.8;
    }
};
*/

future<results> test_load_balancing_with_many_tables(params p, bool tablet_aware) {
    auto cfg = tablet_cql_test_config();
    results global_res;
    co_await do_with_cql_env_thread([&] (auto& e) {
        const int n_hosts = p.nodes;
        const shard_id shard_count = p.shards;
        const int cycles = p.iterations;
        std::unordered_map<global_tablet_id, uint64_t> tablet_sizes;

        auto rack1 = endpoint_dc_rack{ dc, "rack-1" };

        std::vector<host_id> hosts;
        std::vector<inet_address> ips;

        int host_seq = 1;
        auto add_host = [&] {
            hosts.push_back(host_id(utils::UUID(::format("{:02x}0000000-0000-0000-0000-000000000000", host_seq))));
            ips.push_back(inet_address(format("192.168.0.{}", host_seq++)));
            testlblog.info("Added new node: {} ({})", hosts.back(), ips.back());
        };

        dht::token t{0};
        auto add_host_to_topology = [&] (token_metadata& tm, int i) -> future<> {
            tm.update_topology(hosts[i], rack1, node::state::normal, shard_count);
            t._data += 10000;
            co_await tm.update_normal_tokens(std::unordered_set{t}, hosts[i]);
        };

        for (int i = 0; i < n_hosts; ++i) {
            add_host();
        }

        semaphore sem(1);
        auto stm = shared_token_metadata([&sem]() noexcept { return get_units(sem, 1); }, locator::token_metadata::config {
                locator::topology::config {
                        .this_endpoint = ips[0],
                        .this_host_id = hosts[0],
                        .local_dc_rack = rack1
                }
        });

        auto bootstrap = [&] {
            stm.mutate_token_metadata([&] (token_metadata& tm) {
                add_host();
                return add_host_to_topology(tm, hosts.size() - 1);
            }).get();
            global_res.stats += rebalance_tablets(e.get_tablet_allocator().local(), stm, tablet_sizes);
        };

        auto decommission = [&] (host_id host) {
            auto i = std::distance(hosts.begin(), std::find(hosts.begin(), hosts.end(), host));
            if ((size_t)i == hosts.size()) {
                throw std::runtime_error(format("No such host: {}", host));
            }
            stm.mutate_token_metadata([&] (token_metadata& tm) {
                tm.update_topology(hosts[i], rack1, locator::node::state::being_decommissioned, shard_count);
                return make_ready_future<>();
            }).get();

            global_res.stats += rebalance_tablets(e.get_tablet_allocator().local(), stm, tablet_sizes);

            stm.mutate_token_metadata([&] (token_metadata& tm) {
                tm.remove_endpoint(host);
                return make_ready_future<>();
            }).get();
            testlblog.info("Node decommissioned: {} ({})", hosts[i], ips[i]);
            hosts.erase(hosts.begin() + i);
            ips.erase(ips.begin() + i);
        };

        stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
            for (int i = 0; i < n_hosts; ++i) {
                co_await add_host_to_topology(tm, i);
            }
        }).get();

        auto allocate = [&] (schema_ptr s, int rf, std::optional<int> initial_tablets) {
            replication_strategy_config_options opts;
            opts[rack1.dc] = format("{}", rf);
            network_topology_strategy tablet_rs(replication_strategy_params(opts, initial_tablets));
            stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
                auto map = co_await tablet_rs.allocate_tablets_for_new_table(s, stm.get(), service::default_target_tablet_size);
                tm.tablets().set_tablet_map(s->id(), std::move(map));
            }).get();
        };

        auto id1 = add_table(e).get();
        auto id2 = add_table(e).get();
        schema_ptr s1 = e.local_db().find_schema(id1);
        schema_ptr s2 = e.local_db().find_schema(id2);
        allocate(s1, p.rf1, p.tablets1);
        allocate(s2, p.rf2, p.tablets2);

        // allocate sizes to tablets
        tablet_size_generator tsg(p.seed);
        uint64_t tablet_size_sum = 0;
        std::unordered_map<table_id, uint64_t> table_sizes;
        for (auto&& [table, tmap_] : stm.get()->tablets().all_tables()) {
            auto& tmap = *tmap_;
            uint64_t total_table_size = 0;
            tmap.for_each_tablet([&] (tablet_id tid, const tablet_info& ti) -> future<> {
                const uint64_t tablet_size = tsg.generate();
                global_tablet_id gtid{table, tid};
                tablet_sizes[gtid] = tablet_size;

                tablet_size_sum += tablet_size;
                total_table_size += tablet_size;
                if (tablet_size > default_target_tablet_size * 4) {
                    testlblog.info("huge tablet: {}:{} {}", table, tid, size2gb(tablet_size));
                }
                return make_ready_future<>();
            }).get();

            table_sizes[table] = total_table_size * (table == s1->id() ? p.rf1 : p.rf2);
        }

        testlblog.info("created {} tablet sizes; avg size: {}", tablet_sizes.size(), size2gb(tablet_size_sum / tablet_sizes.size(), 2));

        auto check_balance = [&] () -> cluster_balance {
            cluster_balance res;

            testlblog.debug("tablet metadata: {}", stm.get()->tablets());

            locator::cluster_disk_usage disk_usage = get_cluster_with_capacities(stm.get());
            uint64_t cluster_capacity = 0;
            for (const auto& [host_id, hls]: disk_usage) {
                uint64_t host_capacity = 0;
                for (const locator::disk_usage& du : hls.usage_by_shard | std::views::values) {
                    host_capacity += du.capacity;
                }
                if (!stm.get()->get_topology().get_node(host_id).is_leaving()) {
                    cluster_capacity += host_capacity;
                }
            }
    
            int table_index = 0;
            for (auto s : {s1, s2}) {
                locator::cluster_disk_usage disk_usage = calc_disk_usage(stm.get(), tablet_sizes);
                size_load_sketch size_load(stm.get(), disk_usage);
                size_load.populate(std::nullopt, s->id()).get();

                min_max_tracker<double> node_load_minmax;
                for (auto h: hosts) {
                    const auto [node_used, node_capacity] = size_load.get_disk_usage(h);
                    const uint64_t ideal_table_size = double(node_capacity) / cluster_capacity * table_sizes.at(s->id());
                    const double node_load = 100.0 * node_used / ideal_table_size;
                    node_load_minmax.update(node_load);
                    testlblog.info("Table load on {:.2} for {:.1}: size={} ideal={:5.1f}%",
                                ::format("{}", h), s->cf_name(), size2gb(node_used), node_load);
                }

                auto node_imbalance = node_load_minmax.max() - node_load_minmax.min();
                testlblog.info("Table node imbalance: min={:5.1f}%, max={:5.1f}%, spread={:5.1f}%",
                              node_load_minmax.min(), node_load_minmax.max(), node_imbalance);

                res.tables[table_index++] = {
                    .shard_overcommit = 0,
                    .best_shard_overcommit = 0,
                    .node_overcommit = 0
                };
            }

            disk_usage = calc_disk_usage(stm.get(), tablet_sizes);
            size_load_sketch size_load(stm.get(), disk_usage);
            size_load.populate(std::nullopt).get();

            for (auto h: hosts) {
                auto node_du = size_load.get_disk_usage(h);
                testlblog.info("Node load {:.2} used={} cap={} disk_load={:5.1f}%",
                    ::format("{}", h), size2gb(node_du.used), size2gb(node_du.capacity), 100.0 * node_du.used / node_du.capacity);
            }

            /*
            for (int i = 0; i < nr_tables; i++) {
                auto t = res.tables[i];
                global_res.worst.tables[i].shard_overcommit = std::max(global_res.worst.tables[i].shard_overcommit, t.shard_overcommit);
                global_res.worst.tables[i].node_overcommit = std::max(global_res.worst.tables[i].node_overcommit, t.node_overcommit);
            }
            testlblog.info("Overcommit: {}", res);
            */

            return res;
        };

        testlblog.debug("tablet metadata: {}", stm.get()->tablets());

        e.get_tablet_allocator().local().set_use_table_aware_balancing(tablet_aware);

        check_balance();

        rebalance_tablets(e.get_tablet_allocator().local(), stm, tablet_sizes);

        global_res.init = global_res.worst = check_balance();

        for (int i = 0; i < cycles; i++) {
            bootstrap();
            check_balance();

            decommission(hosts[0]);
            global_res.last = check_balance();
        }
    }, cfg);
    co_return global_res;
}

future<> run_simulation(const params& p, const sstring& name = "") {
    testlblog.info("[run {}] params: {}", name, p);

    auto total_tablet_count = p.tablets1.value_or(0) * p.rf1 + p.tablets2.value_or(0) * p.rf2;
    testlblog.info("[run {}] tablet count: {}", name, total_tablet_count);
    testlblog.info("[run {}] tablet count / shard: {:.3f}", name, double(total_tablet_count) / (p.nodes * p.shards));

    auto res = co_await test_load_balancing_with_many_tables(p, true);
    testlblog.info("[run {}] Overcommit       : init : {}", name, res.init);
    testlblog.info("[run {}] Overcommit       : worst: {}", name, res.worst);
    testlblog.info("[run {}] Overcommit       : last : {}", name, res.last);
    testlblog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 res.stats.elapsed_time.count(), res.stats.max_rebalance_time.count(), res.stats.rebalance_count);

    if (res.stats.elapsed_time > seconds_double(1)) {
        testlblog.warn("[run {}] Scheduling took longer than 1s!", name);
    }

    /*
    auto old_res = co_await test_load_balancing_with_many_tables(p, false);
    testlblog.info("[run {}] Overcommit (old) : init : {}", name, old_res.init);
    testlblog.info("[run {}] Overcommit (old) : worst: {}", name, old_res.worst);
    testlblog.info("[run {}] Overcommit (old) : last : {}", name, old_res.last);
    testlblog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 old_res.stats.elapsed_time.count(), old_res.stats.max_rebalance_time.count(), old_res.stats.rebalance_count);

    for (int i = 0; i < nr_tables; ++i) {
        if (res.worst.tables[i].shard_overcommit > old_res.worst.tables[i].shard_overcommit) {
            testlblog.warn("[run {}] table{} shard overcommit worse!", name, i + 1);
        }
        auto overcommit = res.worst.tables[i].shard_overcommit;
        if (overcommit > 1.2) {
            testlblog.warn("[run {}] table{} shard overcommit {:.2f} > 1.2!", name, i + 1, overcommit);
        }
    }
    */
}

future<> run_simulations(const boost::program_options::variables_map& app_cfg) {
    for (auto i = 0; i < app_cfg["runs"].as<int>(); i++) {
        auto shards = 1 << tests::random::get_int(0, 8);
        auto rf1 = tests::random::get_int(1, 3);
        auto rf2 = tests::random::get_int(1, 3);
        auto scale1 = 1 << tests::random::get_int(0, 5);
        auto scale2 = 1 << tests::random::get_int(0, 5);
        auto nodes = tests::random::get_int(3, 6);
        params p {
            .iterations = app_cfg["iterations"].as<int>(),
            .nodes = nodes,
            .tablets1 = std::bit_ceil<size_t>(div_ceil(shards * nodes, rf1) * scale1),
            .tablets2 = std::bit_ceil<size_t>(div_ceil(shards * nodes, rf2) * scale2),
            .rf1 = rf1,
            .rf2 = rf2,
            .shards = shards,
            .scale1 = scale1,
            .scale2 = scale2,
        };

        auto name = format("#{}", i);
        co_await run_simulation(p, name);
    }
}

namespace perf {

int scylla_tablet_load_balancing_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
            ("runs", bpo::value<int>(), "Number of simulation runs.")
            ("iterations", bpo::value<int>()->default_value(8), "Number of topology-changing cycles in each run.")
            ("nodes", bpo::value<int>(), "Number of nodes in the cluster.")
            ("tablets1", bpo::value<int>(), "Number of tablets for the first table.")
            ("tablets2", bpo::value<int>(), "Number of tablets for the second table.")
            ("rf1", bpo::value<int>(), "Replication factor for the first table.")
            ("rf2", bpo::value<int>(), "Replication factor for the second table.")
            ("shards", bpo::value<int>(), "Number of shards per node.")
            ("seed", bpo::value<int>(), "Random seed.")
            ("verbose", "Enables standard logging")
            ;
    return app.run(argc, argv, [&] {
        return seastar::async([&] {
            if (!app.configuration().contains("verbose")) {
                auto testlog_level = logging::logger_registry().get_logger_level("testlblog");
                logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
                logging::logger_registry().set_logger_level("testlblog", testlog_level);
                logging::logger_registry().set_logger_level(yellow("dbglog"), seastar::log_level::info);
            }
            engine().at_exit([] {
                aborted.request_abort();
                return make_ready_future();
            });
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            try {
                if (app.configuration().contains("runs")) {
                    run_simulations(app.configuration()).get();
                } else {
                    params p {
                        .iterations = app.configuration()["iterations"].as<int>(),
                        .nodes = app.configuration()["nodes"].as<int>(),
                        .tablets1 = app.configuration()["tablets1"].as<int>(),
                        .tablets2 = app.configuration()["tablets2"].as<int>(),
                        .rf1 = app.configuration()["rf1"].as<int>(),
                        .rf2 = app.configuration()["rf2"].as<int>(),
                        .shards = app.configuration()["shards"].as<int>(),
                    };

                    if (app.configuration().count("seed")) {
                        p.seed = app.configuration()["seed"].as<int>();
                    }
                    run_simulation(p).get();
                }
            } catch (seastar::abort_requested_exception&) {
                // Ignore
            }
        });
    });
}

} // namespace perf
