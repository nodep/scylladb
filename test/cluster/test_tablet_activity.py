#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
End-to-end integration test for activity-weighted tablet allocation.

The C++ unit tests in test/boost/tablets_test.cc exercise the allocator
algorithm in isolation by synthesizing load_stats.table_activity directly.
This test complements them by running a real cluster: per-shard EWMAs are
populated by actual CQL writes, the storage_service aggregates them into
load_stats, the topology coordinator ships the stats to the load balancer,
and the tablet allocator consumes the real data. It verifies that a hot
table retains more tablets than cold, empty tables after rebalance.
"""
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_count
from test.cluster.util import new_test_keyspace

import asyncio
import contextlib
import logging
import pytest

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_activity_weighted_allocation(manager: ManagerClient):
    # The allocator's activity-weighted Phase 3 scaling kicks in when the sum
    # of tablet replicas per shard exceeds tablets_per_shard_goal. We force
    # that situation by creating several tables with a generous 'initial'
    # tablet count against a small goal. With activity weighting enabled,
    # writes to one table must skew the budget toward it.

    cfg = {
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        'tablets_per_shard_goal': 2,
        'tablets_per_shard_hard_limit_multiplier': 4,
        'tablets_activity_weighted_allocation_enabled': True,
        # Ensure we can distinguish an active table from idle ones after a
        # short write burst. A single EWMA tick (10 seconds) suffices to lift
        # the rate above the default active threshold of 1.0 ops/sec.
        'tablets_active_table_rate_threshold': 1.0,
        'tablets_idle_table_rate_threshold': 0.1,
    }
    cmdline = [
        '--smp', '1',
        '--logger-log-level', 'load_balancer=debug',
    ]

    servers = [await manager.server_add(config=cfg, cmdline=cmdline)]
    cql = manager.get_cql()

    # Disable balancing while we set the scene so we don't race with the
    # allocator while creating tables.
    await manager.disable_tablet_balancing()

    # Four keyspaces, each starting at 4 tablets => 16 tablets on 1 shard,
    # well above the goal of 2 so Phase 3 will definitely run.
    async with contextlib.AsyncExitStack() as stack:
        keyspaces = []
        for _ in range(4):
            ks = await stack.enter_async_context(new_test_keyspace(manager,
                    "WITH replication = {'class': 'NetworkTopologyStrategy', "
                    "'replication_factor': 1} AND tablets = {'initial': 4}"))
            keyspaces.append(ks)

        for ks in keyspaces:
            await cql.run_async(
                    f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

        hot_ks = keyspaces[0]
        cold_kss = keyspaces[1:]

        # Generate sustained write load on the hot table. The EWMA ticks once
        # per 10 seconds; we cover at least two ticks (>= 20 s) so that the
        # reported rate is well above 1.0 ops/sec on the node's shard. Cold
        # tables receive no load at all so their rate stays at 0.
        insert_hot = cql.prepare(f"INSERT INTO {hot_ks}.t (pk, v) VALUES (?, ?)")

        async def drive_load(duration_s: float):
            end = asyncio.get_running_loop().time() + duration_s
            i = 0
            while asyncio.get_running_loop().time() < end:
                # Small batch of writes, then yield to the event loop. This
                # gives the EWMA tick timer room to fire while we keep adding
                # to the counters across multiple tick windows.
                await asyncio.gather(*[
                    cql.run_async(insert_hot, [i + k, i + k]) for k in range(20)
                ])
                i += 20
            logger.info(f"Generated {i} writes on {hot_ks}")

        logger.info("Driving write load on hot table for 25 seconds to let "
                    "the per-table EWMA stabilize above the active threshold")
        await drive_load(25.0)

        # Give the load-stats refresh and the topology coordinator a chance
        # to pick up the activity-rich load_stats before we re-enable
        # balancing and let Phase 3 apply it.
        s0_log = await manager.server_open_log(servers[0].server_id)
        s0_mark = await s0_log.mark()
        await s0_log.wait_for('Refreshed table load stats for all DC',
                              from_mark=s0_mark)

        await manager.enable_tablet_balancing()
        # quiesce_topology waits for any in-flight topology work (including
        # a rebalance pass) to complete.
        await manager.api.quiesce_topology(servers[0].ip_addr)

        hot_count = await get_tablet_count(manager, servers[0], hot_ks, 't')
        cold_counts = [
            await get_tablet_count(manager, servers[0], ks, 't')
            for ks in cold_kss
        ]
        logger.info(f"After rebalance: hot={hot_count}, cold={cold_counts}")

        # The activity-weighted allocator must preserve more tablets on the
        # hot table than on any idle table.
        for c in cold_counts:
            assert hot_count > c, (
                f"hot={hot_count} must exceed each cold={cold_counts} with "
                "activity-weighted allocation enabled")

        # And the idle tables should be compressed relative to their initial
        # count of 4 (since they have no activity and no data).
        for c in cold_counts:
            assert c < 4, (
                f"idle tables should shrink below their initial count of 4; "
                f"got {cold_counts}")

        # Total per-shard count stays within the hard limit
        # (goal * multiplier = 2 * 4 = 8), allowing some slack for pow2
        # rounding applied after Phase 3.
        total = hot_count + sum(cold_counts)
        assert total <= 16, (
            f"per-shard total {total} exceeds hard-limit slack; "
            f"hot={hot_count}, cold={cold_counts}")
