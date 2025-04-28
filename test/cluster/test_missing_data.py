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

    inital_tablets = 32

    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{'initial': {inital_tablets}}}")
    await cql.run_async('CREATE TABLE ks.test (pk int PRIMARY KEY, c int);')

    for s in servers:
        await manager.api.disable_autocompaction(s.ip_addr, 'ks', 'test')

    num_records = inital_tablets

    for i in range(num_records):
        cql.execute(f'INSERT INTO ks.test (pk, c) VALUES ({i}, {i})')

    await manager.api.flush_keyspace(servers[0].ip_addr, 'ks')

    # logger.info('tokens:')
    # for row in await cql.run_async('SELECT token(pk), pk FROM ks.test', host=hosts[0]):
    #     logger.info(row)

    s = ''
    for row in await cql.run_async(f"SELECT * FROM system.tablets WHERE table_name = 'test' ALLOW FILTERING"):
        s += f'{row}\n'
    logger.info(f'original tablets:\n{s}')

    tablets_cnt = inital_tablets

    while True:
        # allow merge
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, 'merge_for_missing_data', one_shot=False, parameters={'tablets_cnt': tablets_cnt})

        while True:
            s = ''
            act_tablet_count = 0
            for row in await cql.run_async(f"SELECT * FROM system.tablets WHERE table_name = 'test' ALLOW FILTERING"):
                s += f'{row}\n'
                act_tablet_count += 1
            logger.info(f'actual/expected tablet count: {act_tablet_count}/{tablets_cnt // 2}')

            if act_tablet_count == tablets_cnt // 2:
                tablets_cnt = act_tablet_count
                logger.info(f'tablets:\n{s}')
                break

            await asyncio.sleep(.1)

        logger.info('merged!!!')

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
            if missing:
                logger.info(f'missing: {missing}')

                '''
                # run with tracing
                try:
                    logger.info(f'Running tracing for query {qry}')
                    res = cql.execute(qry, trace=True, host=hosts[i])
                    tracing = res.get_all_query_traces(max_wait_sec_per=900)
                    page_traces = []
                    for trace in tracing:
                        trace_events = []
                        for event in trace.events:
                            trace_events.append(f"  {event.source} {event.source_elapsed} {event.description}")
                        page_traces.append("\n".join(trace_events))
                    logger.info("Tracing {}:\n{}\n".format(qry, "\n".join(page_traces)))
                    for row in res:
                        logger.info(f'  data row: {row}')
                except Exception as ex:
                    self.log.warning(f'  exception: {ex}')
                '''

            assert rec_count == num_records, f"received {rec_count} records instead of {num_records} while querying server {servers[i].server_id}; tablet count {tablets_cnt}"

        await asyncio.sleep(.1)
