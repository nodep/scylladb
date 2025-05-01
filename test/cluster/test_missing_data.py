#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace
from test.pylib.util import wait_for_cql_and_get_hosts
import time
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_missing_data(manager: ManagerClient):

    logger.info('Bootstrapping cluster')
    cfg = { 'enable_tablets': True,
            'error_injections_at_startup': ['short_tablet_stats_refresh_interval']
            }
    cmdline = [
        '--logger-log-level', 'load_balancer=debug',
        '--logger-log-level', 'debug_error_injection=debug',
        '--smp', '2',
    ]
    server = await manager.server_add(cmdline=cmdline, config=cfg)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    await manager.api.disable_tablet_balancing(server.ip_addr)

    inital_tablets = 4

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': {inital_tablets}}}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);')

        await manager.api.disable_autocompaction(server.ip_addr, ks, 'test')

        # insert data
        num_records = 7
        for k in range(num_records):
            cql.execute(f'INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k})')

        # read the data back to get the token IDs
        rows_with_tokens = []
        for row in await cql.run_async(f'SELECT token(pk), pk FROM {ks}.test'):
            logger.info(f'row: token={row.system_token_pk} pk={row.pk}')
            rows_with_tokens.append((row.system_token_pk, row.pk))

        # flush the table
        await manager.api.flush_keyspace(server.ip_addr, ks)

        # force merge on the test table
        # this injection also disables split on the test table
        merge_tablets_cnt = inital_tablets
        await manager.api.enable_injection(server.ip_addr, 'merge_for_missing_data', one_shot=False, parameters={'merge_tablets_cnt': merge_tablets_cnt})
        await manager.api.enable_tablet_balancing(server.ip_addr)

        # wait for merge to complete
        expect_tablet_count = merge_tablets_cnt // 2
        started = time.time()
        while True:
            s = ''
            act_tablet_count = 0
            rwt_index = 0
            for row in await cql.run_async(f"SELECT * FROM system.tablets WHERE table_name = 'test' ALLOW FILTERING"):
                while rwt_index < len(rows_with_tokens) and rows_with_tokens[rwt_index][0] <= row.last_token:
                    s += f'  row: token={rows_with_tokens[rwt_index][0]} pk={rows_with_tokens[rwt_index][1]}\n'
                    rwt_index += 1
                s += f'{row}\n'
                act_tablet_count += 1

            logger.debug(f'actual/expected tablet count: {act_tablet_count}/{expect_tablet_count}')

            if act_tablet_count == expect_tablet_count:
                logger.info(f'tablets/rows:\n{s}')
                break

            if time.time() - started > 60:
                assert False, 'Timeout while waiting for tablet merge'

            await asyncio.sleep(.1)

        logger.info(f'Merged test table; new number of tablets: {expect_tablet_count}')

        # assert that the number of records has not changed
        qry = f'SELECT * FROM {ks}.test'
        logger.info(f'Running: {qry}')
        res = cql.execute(qry)
        missing = set(range(num_records))
        rec_count = 0
        for row in res:
            rec_count += 1
            missing.discard(row.pk)

        assert rec_count == num_records, f"received {rec_count} records instead of {num_records} while querying server {server.server_id}; missing keys: {missing}"
        assert False
