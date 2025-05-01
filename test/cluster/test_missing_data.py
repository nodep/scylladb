#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.protocol import InvalidRequest
from cassandra.cluster import TruncateError
from cassandra.policies import FallthroughRetryPolicy
from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.cluster.util import get_topology_coordinator, new_test_keyspace
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for_cql_and_get_hosts
import time
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_missing_data(manager: ManagerClient):

    logger.info('Bootstrapping cluster')
    cfg = { 'enable_tablets': True,
            'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
            }
    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
        '--logger-log-level', 'debug_error_injection=debug',
    ]
    servers = []
    for i in range(3):
        servers.append(await manager.server_add(cmdline=cmdline, config=cfg))

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    # create a table with 32 tablets and disable auto compaction
    inital_tablets = 32

    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'initial': {inital_tablets}}}")
    await cql.run_async('CREATE TABLE ks.test (pk int PRIMARY KEY, c int);')

    for s in servers:
        await manager.api.disable_autocompaction(s.ip_addr, 'ks', 'test')

    # insert one record per tablet on average
    num_records = inital_tablets

    for i in range(num_records):
        cql.execute(f'INSERT INTO ks.test (pk, c) VALUES ({i}, {i})')

    # flush everything
    for s in servers:
        await manager.api.flush_keyspace(servers[0].ip_addr, 'ks')

    # force merge on the test table
    merge_tablets_cnt = inital_tablets
    for s in servers:
        await manager.api.enable_injection(s.ip_addr, 'merge_for_missing_data', one_shot=False, parameters={'merge_tablets_cnt': merge_tablets_cnt})

    await manager.api.enable_tablet_balancing(servers[0].ip_addr)

    # wait for merge to complete
    expect_tablet_count = merge_tablets_cnt // 2
    started = time.time()
    while True:
        s = ''
        act_tablet_count = 0
        for row in await cql.run_async(f"SELECT * FROM system.tablets WHERE table_name = 'test' ALLOW FILTERING"):
            s += f'{row}\n'
            act_tablet_count += 1
        logger.info(f'actual/expected tablet count: {act_tablet_count}/{expect_tablet_count}')

        if act_tablet_count == expect_tablet_count:
            logger.info(f'tablets:\n{s}')
            break

        if (time.time() - started) > 60:
            assert False, 'Timeout while waiting for tablet merge'

        await asyncio.sleep(.1)

    logger.info(f'merged test table; new number of tablets: {expect_tablet_count}')

    # assrert the number of records has not changed
    for i in range(len(hosts)):
        qry = 'SELECT * FROM ks.test'
        stmt = SimpleStatement(qry, consistency_level = ConsistencyLevel.ONE)
        logger.info(f'Running: "{qry}" on server ID: {servers[i].server_id}')
        res = cql.execute(stmt, host=hosts[i])
        missing = set(range(num_records))
        rec_count = 0
        for row in res:
            rec_count += 1
            missing.discard(row.pk)

        assert rec_count == num_records, f"received {rec_count} records instead of {num_records} while querying server {servers[i].server_id}; missing keys: {missing}"

    await asyncio.sleep(.1)
