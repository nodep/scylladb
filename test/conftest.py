#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from random import randint
from typing import TYPE_CHECKING

import pytest

from test import ALL_MODES, TEST_RUNNER, TOP_SRC_DIR
from test.pylib.report_plugin import ReportPlugin
from test.pylib.util import get_configured_modes
from test.pylib.suite.base import (
    TestSuite,
    find_suite_config,
    init_testsuite_globals,
    prepare_dirs,
    start_3rd_party_services,
)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from test.pylib.cpp.item import CppTestFunction
    from test.pylib.suite.base import Test


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption('--mode', choices=ALL_MODES, action="append", dest="modes",
                     help="Run only tests for given build mode(s)")
    parser.addoption('--tmpdir', action='store', default=str(TOP_SRC_DIR / 'testlog'),
                     help='Path to temporary test data and log files.  The data is further segregated per build mode.')
    parser.addoption('--run_id', action='store', default=None, help='Run id for the test run')
    parser.addoption('--byte-limit', action="store", default=randint(0, 2000), type=int,
                     help="Specific byte limit for failure injection (random by default)")

    # Following option is to use with bare pytest command.
    #
    # For compatibility with reasons need to run bare pytest with  --test-py-init option
    # to run a test.py-compatible pytest session.
    #
    # TODO: remove this when we'll completely switch to bare pytest runner.
    parser.addoption('--test-py-init', action='store_true', default=False,
                     help='Run pytest session in test.py-compatible mode.  I.e., start all required services, etc.')

    # Options for compatibility with test.py
    parser.addoption('--save-log-on-success', default=False,
                        dest="save_log_on_success", action="store_true",
                        help="Save test log output on success.")
    parser.addoption('--coverage', action='store_true', default=False,
                      help="When running code instrumented with coverage support"
                           "Will route the profiles to `tmpdir`/mode/coverage/`suite` and post process them in order to generate "
                           "lcov file per suite, lcov file per mode, and an lcov file for the entire run, "
                           "The lcov files can eventually be used for generating coverage reports")
    parser.addoption("--cluster-pool-size", type=int,
                     help="Set the pool_size for PythonTest and its descendants.  Alternatively environment variable "
                          "CLUSTER_POOL_SIZE can be used to achieve the same")
    parser.addoption("--extra-scylla-cmdline-options", default=[],
                     help="Passing extra scylla cmdline options for all tests.  Options should be space separated:"
                          " '--logger-log-level raft=trace --default-log-level error'")


@pytest.fixture(scope="session")
def build_mode(request: pytest.FixtureRequest) -> str:
    """
    This fixture returns current build mode.
    This is for running tests through the test.py script, where only one mode is passed to the test
    """
    # to avoid issues when there's no provided mode parameter, do it in two steps: get the parameter and if it's not
    # None, get the first value from the list
    mode = request.config.getoption("modes")
    if mode:
        return mode[0]
    return mode


@pytest.fixture(scope="module")
async def testpy_testsuite(request: pytest.FixtureRequest, build_mode: str) -> TestSuite:
    suite_config = find_suite_config(path=request.path)
    return TestSuite.opt_create(path=str(suite_config.parent), options=request.config.option, mode=build_mode)


@pytest.fixture(scope="module")
async def testpy_test(request: pytest.FixtureRequest, testpy_testsuite: TestSuite) -> Test:
    await testpy_testsuite.add_test(shortname=request.node.name, casename=None)
    return testpy_testsuite.tests[-1]  # most recent test added to the test suite; i.e., by the previous line.


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(ReportPlugin())


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item | CppTestFunction]) -> None:
    """
    This is a standard pytest method.
    This is needed to modify the test names with dev mode and run id to differ them one from another
    """
    run_id = config.getoption('run_id', None)

    for item in items:
        # check if this is custom cpp tests that have additional attributes for name modification
        if hasattr(item, 'mode'):
            # modify name with mode that is always present in cpp tests
            item.nodeid = f'{item.nodeid}.{item.mode}'
            item.name = f'{item.name}.{item.mode}'
            if item.run_id:
                item.nodeid = f'{item.nodeid}.{item.run_id}'
                item.name = f'{item.name}.{item.run_id}'
        else:
            # here go python tests that are executed through test.py
            # since test.py is responsible for creating several tests with the required mode,
            # a list with modes contains only one value,
            # that's why in name modification the first element is used
            modes = config.getoption('modes')
            if modes:
                item._nodeid = f'{item._nodeid}.{modes[0]}'
                item.name = f'{item.name}.{modes[0]}'
            if run_id:
                item._nodeid = f'{item._nodeid}.{run_id}'
                item.name = f'{item.name}.{run_id}'


def pytest_sessionstart(session: pytest.Session) -> None:
    # test.py starts S3 mock and create/cleanup testlog by itself. Also, if we run with --collect-only option,
    # we don't need this stuff.
    if TEST_RUNNER != "pytest" or session.config.getoption("--collect-only"):
        return

    if not session.config.getoption("--test-py-init"):
        return

    init_testsuite_globals()
    TestSuite.artifacts.add_exit_artifact(None, TestSuite.hosts.cleanup)

    # Run stuff just once for the pytest session even running under xdist.
    if "xdist" not in sys.modules or not sys.modules["xdist"].is_xdist_worker(request_or_session=session):
        temp_dir = Path(session.config.getoption("--tmpdir")).absolute()
        prepare_dirs(tempdir_base=temp_dir, modes=session.config.getoption("--mode") or get_configured_modes())
        start_3rd_party_services(tempdir_base=temp_dir, toxyproxy_byte_limit=session.config.getoption("byte_limit"))


def pytest_sessionfinish() -> None:
    if getattr(TestSuite, "artifacts", None) is not None:
        asyncio.get_event_loop().run_until_complete(TestSuite.artifacts.cleanup_before_exit())
