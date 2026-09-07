#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""
A group0 operation's timeout has to cover the mutexes start_operation() takes, not just
the read barrier between them.

service::raft_group0_client::start_operation() serialises on three things in this order:

    hold_mutex(_operation_mutex, ..., as, timeout);   // (a)
    read_barrier(&as, timeout);                       // (b)
    hold_read_apply_mutex(as, timeout);               // (c)

(a) and (c) used to take only the caller's abort source, which for the migration_manager
callers is the module-level `_as`, i.e. bounded only by shutdown; `timeout` covered (b)
alone.

Both mutexes are semaphore(1)s that a group0_guard holds for the whole operation, commit
included -- topology_coordinator::update_topology_state() passes the guard by value into
add_entry() and gives it no timeout of its own -- and _read_apply_mutex is taken by
group0_state_machine::apply() as well, without _operation_mutex. So a single group0
operation that cannot complete would block every subsequent one on that node
indefinitely, whatever timeout each of them carried, and callers relying on the timeout
to surface an error hung instead of failing.

This test parks a keyspace_rf_change on the coordinator at the point where it holds the
guard and is about to commit, using the wait-before-committing-rf-change-event injection
that already exists for that purpose, then issues a CREATE TABLE on that same node. The
DDL carries group0_raft_op_timeout_in_ms via
migration_manager::start_group0_operation(), which always passes at least a default
raft_timeout, so it has to resolve within that budget rather than wait out the holder.

Needs no quorum loss and no auto-RF; the injection only makes the holder's slowness
deterministic and bounded. Any genuinely slow or stuck group0 operation has the same
effect, which is what made this reachable in production -- see the quorum-loss case,
where the holder is an add_entry() carrying no timeout at all.
"""

import asyncio
import logging
import time

import pytest

from test.cluster.util import get_topology_coordinator
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

GROUP0_OP_TIMEOUT_MS = 3000
# How long the holder keeps the guard. Must stay below the injection's own 30s
# wait_for_message window, past which the node aborts.
HOLD_S = 12

INJECTION = "wait-before-committing-rf-change-event"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.asyncio
async def test_group0_operation_timeout_covers_the_group0_mutexes(manager: ScyllaClusterManager):
    cfg = {
        # Master equivalence: keep the system keyspaces on vnodes so auto-RF is not a
        # participant. The bug is in the group0 client, not in auto-RF.
        'error_injections_at_startup': ['auto_rf_keyspaces_use_vnodes'],
    }
    property_file = [{"dc": "dc1", "rack": f"rack{i}"} for i in range(1, 4)]
    servers = await manager.servers_add(3, config=cfg, property_file=property_file)
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    coordinator_id = await get_topology_coordinator(manager)
    coord = await manager.find_server_by_host_id(servers, coordinator_id)
    logger.info(f"topology coordinator is {coord.ip_addr}")

    # Shorten the group0 operation timeout only now. Setting it at startup would apply it to
    # cluster formation too, where a legitimate join read-barrier can exceed it in debug
    # under load, and the test would fail in servers_add() rather than on what it asserts.
    # server_update_config only sends SIGHUP, so wait until the node has re-read the file.
    log_file = await manager.server_open_log(coord.server_id)
    mark = await log_file.mark()
    await manager.server_update_config(
            coord.server_id, 'group0_raft_op_timeout_in_ms', GROUP0_OP_TIMEOUT_MS)
    await log_file.wait_for("completed re-reading configuration file", from_mark=mark, timeout=60)

    ks = "g0_timeout_repro"
    await cql.run_async(
        f"CREATE KEYSPACE {ks} WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'dc1': ['rack1']} AND tablets = {'initial': 1}")
    # Baseline: the same DDL on the same node with nothing holding the guard.
    coord_cql = await manager.get_cql_exclusive(coord)
    started = time.time()
    await coord_cql.run_async(f"CREATE TABLE {ks}.seed (pk int PRIMARY KEY)")
    logger.info(f"baseline CREATE TABLE on the coordinator took {time.time() - started:.2f}s")

    # Park the RF change on the coordinator at the point where it holds the group0 guard
    # and is about to commit.
    await manager.api.enable_injection(coord.ip_addr, INJECTION, one_shot=False)

    logger.info("Triggering a keyspace_rf_change (will park holding the group0 guard)")
    alter_fut = cql.run_async(
        f"ALTER KEYSPACE {ks} WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'dc1': ['rack1', 'rack2']}")
    ddl_task = None
    try:
        await manager.api.wait_for_injection_enter(coord.ip_addr, INJECTION)
        logger.info("Coordinator is parked holding the group0 guard")

        # Same node, so the same _operation_mutex. This DDL goes through
        # migration_manager::start_group0_operation(), which always passes at least the
        # default raft_timeout, i.e. group0_raft_op_timeout_in_ms.
        started = time.time()
        ddl_task = asyncio.ensure_future(
            coord_cql.run_async(f"CREATE TABLE {ks}.blocked (pk int PRIMARY KEY)", timeout=120))
        logger.info(f"Issued DDL on the coordinator, giving it {HOLD_S}s "
                    f"to honour its {GROUP0_OP_TIMEOUT_MS}ms group0 timeout")

        done, _ = await asyncio.wait([ddl_task], timeout=HOLD_S)
        elapsed = time.time() - started
        if done:
            outcome = "raised" if ddl_task.exception() else "succeeded"
            logger.info(f"DDL {outcome} after {elapsed:.1f}s: {ddl_task.exception()}")
        else:
            logger.info(f"DDL still blocked after {elapsed:.1f}s")
    finally:
        # Release the holder before the injection's 30s window expires, otherwise the
        # node aborts.
        await manager.api.message_injection(coord.ip_addr, INJECTION)
        await manager.api.disable_injection(coord.ip_addr, INJECTION)
        if ddl_task is not None:
            try:
                released_at = time.time()
                await asyncio.wait_for(ddl_task, timeout=60)
                logger.info(f"DDL completed {time.time() - released_at:.1f}s after the "
                            "holder released the guard")
            except Exception as exc:
                logger.info(f"DDL finished with: {exc}")
        try:
            await asyncio.wait_for(asyncio.ensure_future(alter_fut), timeout=120)
        except Exception as exc:
            logger.info(f"ALTER KEYSPACE finished with: {exc}")

    assert done, (
        f"CREATE TABLE was still blocked {HOLD_S}s after it was issued, despite carrying a "
        f"{GROUP0_OP_TIMEOUT_MS}ms group0 operation timeout. It is waiting for the holder of "
        f"one of the group0 mutexes, which means a wait in start_operation() is no longer "
        f"covered by the caller's timeout.")
