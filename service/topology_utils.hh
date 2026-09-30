/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <seastar/core/future.hh>

namespace db {
class system_keyspace;
}

namespace service {

struct topology;
class group0_guard;

// Whether a keyspace_rf_change for `ks` is queued, paused, being processed or
// has migrations in flight. The guard says that the caller holds group0 and
// so sees a stable topology.
seastar::future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard, seastar::sstring ks);

// The same check without the guard, for callers which deliberately let the
// coordinator make progress while they look (a request may disappear between
// the id being read and its entry being looked up; that counts as not ongoing).
seastar::future<bool> ongoing_rf_change_unguarded(const topology& topology, db::system_keyspace& sys_ks, seastar::sstring ks);

}
