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
#include "locator/load_sketch.hh"
#include "locator/size_load_sketch.hh"
#include "test/lib/topology_builder.hh"
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

constexpr uint64_t huge_tablet_size_threshold = default_target_tablet_size * 10;

static seastar::abort_source aborted;

static const sstring dc = "dc1";

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    return c;
}

static
future<table_id> add_table(cql_test_env& e, sstring test_ks_name = "") {
    static char cnt = 10;
    auto id = table_id(utils::UUID(::format("{:x}0000000-0000-0000-0000-000000000000", cnt++)));
    co_await e.create_table([&] (std::string_view ks_name) {
        if (!test_ks_name.empty()) {
            ks_name = test_ks_name;
        }
        return *schema_builder(ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                .build();
    });
    co_return id;
}

// Run in a seastar thread
static
sstring add_keyspace(cql_test_env& e, std::unordered_map<sstring, int> dc_rf, int initial_tablets = 0) {
    static std::atomic<int> ks_id = 0;
    auto ks_name = fmt::format("keyspace{}", ks_id.fetch_add(1));
    sstring rf_options;
    for (auto& [dc, rf] : dc_rf) {
        rf_options += format(", '{}': {}", dc, rf);
    }
    e.execute_cql(fmt::format("create keyspace {} with replication = {{'class': 'NetworkTopologyStrategy'{}}}"
                              " and tablets = {{'enabled': true, 'initial': {}}}",
                              ks_name, rf_options, initial_tablets)).get();
    return ks_name;
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
        testlog.info("Setting new tablet map of size {}", old_tmap.tablet_count() * 2);
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

static
rebalance_stats rebalance_tablets(cql_test_env& e, locator::load_stats_ptr load_stats,
            const std::unordered_map<global_tablet_id, uint64_t>& tablet_sizes, std::unordered_set<host_id> skiplist = {}) {
    rebalance_stats stats;
    abort_source as;

    auto guard = e.get_raft_group0_client().start_operation(as).get();
    auto& talloc = e.get_tablet_allocator().local();
    auto& stm = e.shared_token_metadata().local();

    // Sanity limit to avoid infinite loops.
    // The x10 factor is arbitrary, it's there to account for more complex schedules than direct migration.
    auto max_iterations = 1 + get_tablet_count(stm.get()->tablets()) * 10;

    for (size_t i = 0; i < max_iterations; ++i) {
        auto prev_lb_stats = talloc.stats().for_dc(dc);
        auto start_time = std::chrono::steady_clock::now();

        auto plan = talloc.balance_tablets(stm.get(), load_stats, skiplist).get();

        auto end_time = std::chrono::steady_clock::now();
        auto lb_stats = talloc.stats().for_dc(dc) - prev_lb_stats;

        uint64_t migrated_gb = 0;
        for (const tablet_migration_info& tmi : plan.migrations()) {
            migrated_gb += tablet_sizes.at(tmi.tablet);
        }
        auto elapsed = std::chrono::duration_cast<seconds_double>(end_time - start_time);
        rebalance_stats iteration_stats = {
            .elapsed_time = elapsed,
            .max_rebalance_time = elapsed,
            .rebalance_count = 1,
            .migrated_gb = migrated_gb,
            .migrated_tablets = plan.size(),
        };
        stats += iteration_stats;
        testlog.debug("Rebalance iteration {} took {:.3f} [s]: mig={}, bad={}, first_bad={}, eval={}, skiplist={}, skip: (load={}, rack={}, node={})",
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
            // We should not introduce inconsistency between on-disk state and in-memory state
            // as that may violate invariants and cause failures in later operations
            // causing test flakiness.
            save_tablet_metadata(e.local_db(), stm.get()->tablets(), guard.write_timestamp()).get();
            e.get_storage_service().local().load_tablet_metadata({}).get();
            testlog.info("Rebalance took {:.3f} [s] after {} iteration(s)", stats.elapsed_time.count(), i + 1);
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

locator::disk_usage host_load_stats::get_sum() const {
    disk_usage result;
    for (const disk_usage& du: usage_by_shard | std::views::values) {
        result.used += du.used;
        result.capacity += du.capacity;
    }
    return result;
}   

uint64_t get_shard_capacity(host_id h) {
    const std::vector<uint64_t> capacities {
        650UL * 1024 * 1024 * 1024,
        550UL * 1024 * 1024 * 1024,
        600UL * 1024 * 1024 * 1024,
        550UL * 1024 * 1024 * 1024,
        550UL * 1024 * 1024 * 1024,
        850UL * 1024 * 1024 * 1024,
        650UL * 1024 * 1024 * 1024,
        550UL * 1024 * 1024 * 1024,
        550UL * 1024 * 1024 * 1024,
        550UL * 1024 * 1024 * 1024,
        850UL * 1024 * 1024 * 1024,
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

                //dbglog("{} {} {}", brief(replica.host), brief(global_tablet_id{table, tid}), size2gb(size, 2));
            }
            return make_ready_future<>();
        }).get();
    }
    
    return disk_usage;
}

struct tablet_size_generator {
    size_t _tablet_count = 0;

    tablet_size_generator(std::optional<unsigned> seed = std::nullopt) {
        const size_t used_seed = seed.value_or(time(NULL));
        testlog.info("random seed: {}", used_seed);
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
            return get(huge_tablet_size_threshold, huge_tablet_size_threshold * 5);
        }
        if (_tablet_count % 51 == 0) {
            return get(default_target_tablet_size * 2, huge_tablet_size_threshold);
        }
        //if (_tablet_count % 200 == 0) {
        //    return get(default_target_tablet_size * 4, huge_tablet_size_threshold);
        //}
        //if (_tablet_count % 101 == 0) {
        //    return get(default_target_tablet_size * 2, default_target_tablet_size * 4);
        //}
        return default_target_tablet_size;
    }
};

double interpolate(double in, const std::vector<interpolate_point>& points) {
    size_t idx = 1;
    while ((idx < points.size() - 1) && (points[idx].in < in)) {
        idx++;
    }

    const interpolate_point& p = points[idx];
    const interpolate_point& last = points[idx - 1];

    return last.out + (in - last.in) * (p.out - last.out)/(p.in - last.in);
}
                                
// raphael's suggestion
double compute_load(const disk_usage& disk_usage, size_t tablet_count, size_t n_shards) {

    const std::vector<interpolate_point> points = {
        /*
        {0.0,  0.2},
        {0.7,  0.2},
        {0.75, 0.18},
        {0.85, 0.15},
        {0.90, 0.11},
        {0.95, 0.06},
        {1.00, 0},
        */
        {0,  0},
        {1,  0},
    };
        
    load_type ideal_tablet_count = disk_usage.used == 0 ? 1.0 : load_type(disk_usage.used) / default_target_tablet_size;
    load_type size_load = load_type(disk_usage.used) / disk_usage.capacity;
    load_type local_count_influence = interpolate(size_load, points);
    load_type count_load = tablet_count / load_type(ideal_tablet_count);

    return size_load * (1 - local_count_influence) + count_load * local_count_influence;
}

future<results> test_load_balancing_with_many_tables(params p, bool tablet_aware) {
    auto cfg = tablet_cql_test_config();
    results global_res;
    co_await do_with_cql_env_thread([&] (auto& e) {
        const int n_hosts = p.nodes;
        const shard_id shard_count = p.shards;
        const int cycles = p.iterations;
        std::unordered_map<global_tablet_id, uint64_t> tablet_sizes;

        topology_builder topo(e);
        std::vector<host_id> hosts;
        locator::load_stats stats;

        auto add_host = [&] {
            auto host = topo.add_node(service::node_state::normal, shard_count);
            hosts.push_back(host);
            stats.capacity[host] = get_shard_capacity(host) * shard_count;
            testlog.info("Added new node: {}", host);
        };

        auto make_stats = [&] {
            return make_lw_shared<locator::load_stats>(stats);
        };

        for (int i = 0; i < n_hosts; ++i) {
            add_host();
        }

        auto& stm = e.shared_token_metadata().local();

        auto bootstrap = [&] {
            add_host();
            global_res.stats += rebalance_tablets(e, make_stats(), tablet_sizes);
        };

        auto decommission = [&] (host_id host) {
            auto i = std::distance(hosts.begin(), std::find(hosts.begin(), hosts.end(), host));
            if ((size_t)i == hosts.size()) {
                throw std::runtime_error(format("No such host: {}", host));
            }
            topo.set_node_state(host, service::node_state::decommissioning);
            global_res.stats += rebalance_tablets(e, make_stats(), tablet_sizes);
            if (stm.get()->tablets().has_replica_on(host)) {
                throw std::runtime_error(format("Host {} still has replicas!", host));
            }
            topo.set_node_state(host, service::node_state::left);
            testlog.info("Node decommissioned: {}", host);
            hosts.erase(hosts.begin() + i);
        };

        auto ks1 = add_keyspace(e, {{topo.dc(), p.rf1}}, p.tablets1.value_or(1));
        auto ks2 = add_keyspace(e, {{topo.dc(), p.rf2}}, p.tablets2.value_or(1));
        auto id1 = add_table(e, ks1).get();
        auto id2 = add_table(e, ks2).get();
        schema_ptr s1 = e.local_db().find_schema(id1);
        schema_ptr s2 = e.local_db().find_schema(id2);

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
                if (tablet_size >= huge_tablet_size_threshold) {
                    testlog.info("huge tablet: {}:{} {}", table, tid, size2gb(tablet_size));
                }
                return make_ready_future<>();
            }).get(); 

            table_sizes[table] = total_table_size * (table == s1->id() ? p.rf1 : p.rf2);
        }
        testlog.info("created {} tablet sizes; avg size: {}", tablet_sizes.size(), size2gb(tablet_size_sum / tablet_sizes.size()));

        auto check_balance = [&] () -> cluster_balance {
            cluster_balance res;

            testlog.debug("tablet metadata: {}", stm.get()->tablets());

            locator::cluster_disk_usage disk_usage = get_cluster_with_capacities(stm.get());
            uint64_t cluster_capacity = 0;
            for (const auto& [host_id, hls]: disk_usage) {
                uint64_t host_capacity = 0;
                for (const locator::disk_usage& du : hls.usage_by_shard | std::views::values) {
                    host_capacity += du.capacity;
                }
                if (!stm.get()->get_topology().get_node(host_id).is_leaving()) {
                    dbglog("adding host {} to capacity {}", host_id, size2gb(host_capacity));
                    cluster_capacity += host_capacity;
                }
            }

            dbglog("cluster capacity {}", size2gb(cluster_capacity));
            for (const auto& [table_id, size] : table_sizes) {
                dbglog("table {} size {}", table_id, size2gb(size, 2));
            }

            int table_index = 0;
            for (auto s : {s1, s2}) {
                dbglog("table {}", s->id());
                locator::cluster_disk_usage disk_usage = calc_disk_usage(stm.get(), tablet_sizes);
                size_load_sketch size_load(stm.get(), disk_usage);
                size_load.populate(std::nullopt, s->id()).get();
    
                min_max_tracker<double> node_load_minmax;
                for (auto h: hosts) {
                    const auto [node_used, node_capacity] = size_load.get_disk_usage(h);
                    const uint64_t ideal_table_size = double(node_capacity) / cluster_capacity * table_sizes.at(s->id());
                    const double node_load = 100.0 * node_used / ideal_table_size;
                    node_load_minmax.update(node_load);
                    testlog.info("Table load on {:.2} for {:.1}: size={} ideal={:5.1f}%",
                                ::format("{}", h), s->cf_name(), size2gb(node_used), node_load);
                    dbglog("host {} cap {} ideal {} used {} ideal {:5.1f}", brief(h), size2gb(node_capacity), size2gb(ideal_table_size),
                                size2gb(node_used), node_load);
                }

                auto node_imbalance = node_load_minmax.max() - node_load_minmax.min();
                testlog.info("Table node imbalance: min={:5.1f}%, max={:5.1f}%, spread={:6.2f}%",
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
    
            min_max_tracker<double> node_load_minmax;
            min_max_tracker<uint64_t> avg_tablet_size_minmax;
            uint64_t total_used = 0;
            size_t num_tablets = 0;
            for (auto h: hosts) {
                auto node_du = size_load.get_disk_usage(h);
                double node_load = 100.0 * node_du.used / node_du.capacity;
                size_t tablet_count = size_load.get_tablet_count(h);
                uint64_t avg_tablet_size = node_du.used / tablet_count;
                testlog.info("Node load {:.2} used={} cap={} disk_load={:5.1f}% count={}, avg_size={}",
                    ::format("{}", h), size2gb(node_du.used), size2gb(node_du.capacity), node_load, tablet_count, size2gb(avg_tablet_size, 2));
                node_load_minmax.update(node_load);
                avg_tablet_size_minmax.update(avg_tablet_size);
                num_tablets += tablet_count;
                total_used += node_du.used;
            }

            const double avg_tablet_size = double(total_used) / num_tablets;
            double min_avg_tablet_size = avg_tablet_size_minmax.min() / avg_tablet_size;
            double max_avg_tablet_size = avg_tablet_size_minmax.max() / avg_tablet_size;

            testlog.info("Nodes load size min={:5.1f}% max={:5.1f}% spread={:5.1f}%  avg_size min={:.2f}% max={:.2f}% spread={:.2f}%",
                    node_load_minmax.min(), node_load_minmax.max(), node_load_minmax.max() - node_load_minmax.min(),
                    min_avg_tablet_size, max_avg_tablet_size, max_avg_tablet_size - min_avg_tablet_size);


            testlog.info("Overcommit: {}", res);
            return res;
        };

        testlog.debug("tablet metadata: {}", stm.get()->tablets());

        e.get_tablet_allocator().local().set_use_table_aware_balancing(tablet_aware);

        check_balance();

        rebalance_tablets(e, make_stats(), tablet_sizes);

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
    testlog.info("[run {}] params: {}", name, p);

    auto total_tablet_count = p.tablets1.value_or(0) * p.rf1 + p.tablets2.value_or(0) * p.rf2;
    testlog.info("[run {}] tablet count: {}", name, total_tablet_count);
    testlog.info("[run {}] tablet count / shard: {:.3f}", name, double(total_tablet_count) / (p.nodes * p.shards));

    auto res = co_await test_load_balancing_with_many_tables(p, true);
    testlog.info("[run {}] Overcommit       : init : {}", name, res.init);
    testlog.info("[run {}] Overcommit       : worst: {}", name, res.worst);
    testlog.info("[run {}] Overcommit       : last : {}", name, res.last);
    testlog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 res.stats.elapsed_time.count(), res.stats.max_rebalance_time.count(), res.stats.rebalance_count);

    if (res.stats.elapsed_time > seconds_double(1)) {
        testlog.warn("[run {}] Scheduling took longer than 1s!", name);
    }

    /*
    auto old_res = co_await test_load_balancing_with_many_tables(p, false);
    testlog.info("[run {}] Overcommit (old) : init : {}", name, old_res.init);
    testlog.info("[run {}] Overcommit (old) : worst: {}", name, old_res.worst);
    testlog.info("[run {}] Overcommit (old) : last : {}", name, old_res.last);
    testlog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 old_res.stats.elapsed_time.count(), old_res.stats.max_rebalance_time.count(), old_res.stats.rebalance_count);

    for (int i = 0; i < nr_tables; ++i) {
        if (res.worst.tables[i].shard_overcommit > old_res.worst.tables[i].shard_overcommit) {
            testlog.warn("[run {}] table{} shard overcommit worse!", name, i + 1);
        }
        auto overcommit = res.worst.tables[i].shard_overcommit;
        if (overcommit > 1.2) {
            testlog.warn("[run {}] table{} shard overcommit {:.2f} > 1.2!", name, i + 1, overcommit);
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
                auto testlog_level = logging::logger_registry().get_logger_level("testlog");
                logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
                logging::logger_registry().set_logger_level("testlog", testlog_level);
                logging::logger_registry().set_logger_level(yellow("dbglog"), testlog_level);
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
