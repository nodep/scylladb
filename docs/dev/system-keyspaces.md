# System Keyspaces Overview

This page gives a high-level overview of several internal keyspaces and what they are used for.

## Table of Contents

- [system_replicated_keys](#system_replicated_keys)
- [system_distributed](#system_distributed)
- [system_distributed_everywhere](#system_distributed_everywhere)
- [system_auth](#system_auth)
- [system](#system)
- [system_schema](#system_schema)
- [system_traces](#system_traces)
- [system_audit/audit](#system_auditaudit)

## `system_replicated_keys`

Internal keyspace for encryption-at-rest key material used by the replicated key provider. It stores encrypted data keys so nodes can retrieve the correct key IDs when reading encrypted data.

This keyspace is created as an internal system keyspace and uses `EverywhereStrategy` so key metadata is available on every node. It is not intended for user data.

## `system_distributed`

Internal distributed metadata keyspace used for cluster-wide coordination data that is shared across nodes.

In practice, it is used for metadata such as:

- materialized view build coordination state
- CDC stream/timestamp metadata exposed to clients
- service level definitions used by workload prioritization

This keyspace is managed by Scylla and is not intended for application tables.
It is created as an internal keyspace (historically with `SimpleStrategy` and RF=3 by default).

## `system_distributed_everywhere`

Legacy keyspace. It is no longer used.

## `system_auth`

Legacy auth keyspace name kept primarily for compatibility.

Auth tables have moved to the `system` keyspace (`roles`, `role_members`, `role_permissions`, and related auth state). `system_auth` may still exist for compatibility with legacy tooling/queries, but it is no longer where current auth state is primarily stored.

## `system`

This keyspace is local one, so each node has its own, independent content for tables in this keyspace. For some tables, the content is coordinated at a higher level (RAFT), but not via the traditional replication systems (storage proxy).

See the detailed table-level documentation here: [system_keyspace](system_keyspace.md)

## `system_schema`

This keyspace is local one, so each node has its own, independent content for tables in this keyspace. All tables in this keyspace are coordinated via the schema replication system.

See the detailed table-level documentation here: [system_schema_keyspace](system_schema_keyspace.md)

## `system_traces`

Internal tracing keyspace used for query tracing and slow-query logging records (`sessions`, `events`, and related index/log tables).

This keyspace is written by Scylla's tracing subsystem for diagnostics and observability. It is operational metadata, not user application data (historically created with `SimpleStrategy` and RF=2).

Where tablets are the default (`tablets_mode_for_new_keyspaces`), the keyspace is created on tablets with `NetworkTopologyStrategy` and RF 1 in every DC (a one-rack rack list where `rf_rack_valid_keyspaces` or `enforce_rack_list` is on; otherwise numeric, which the reconciler converts to a one-rack list as soon as an eligible rack exists), and its replication is then managed by the automatic replication factor reconciler (see below), with a goal of two racks per DC.

## `system_audit`/`audit`

Internal audit-logging keyspace used to persist audit events when table-backed auditing is enabled.

Scylla's audit table storage is implemented as an internal audit keyspace for audit records (for example, auth/admin/DCL activity depending on audit configuration). In current code this keyspace is named `audit`, while operational material may refer to it as its historical name (`system_audit`). It is intended for security/compliance observability, not for application data.

Where tablets are the default, the keyspace is created on tablets with `NetworkTopologyStrategy` and RF 1 in every DC (a one-rack rack list where `rf_rack_valid_keyspaces` or `enforce_rack_list` is on; otherwise numeric, which the reconciler converts to a one-rack list as soon as an eligible rack exists), and its replication is then managed by the automatic replication factor reconciler with a goal of three racks per DC. Where tablets are not the default it is created on vnodes with RF 3 per DC, as before.

### Automatic replication factor of `audit` and `system_traces`

With the `AUTO_REPLICATION_FACTOR` cluster feature, the topology coordinator reconciles the replication of these two keyspaces (`service/topology_coordinator.cc`, `maybe_schedule_auto_rf_change`) against the *eligible* racks: for each DC, the racks in which some non-auto-RF tablets keyspace places replicas (a numeric RF makes every rack of the DC eligible; an RF of 0 makes none). One `keyspace_rf_change` request is issued at a time:

- a numeric RF is converted to a rack list;
- a rack which is no longer eligible is dropped, never a DC's last rack (a DC no eligible keyspace replicates to is shrunk to one rack, never removed), and nothing is dropped while no eligible keyspace exists at all (fresh cluster, last user keyspace dropped);
- an eligible rack with a live node is added while the list is below the goal;
- an eligible DC the keyspace does not replicate to yet is added with one rack.

Changes are deferred while a normal node is dead, unless the node is excluded (`excluded_tablet_nodes`): the barrier skips excluded nodes, and giving up a lost rack is what lets its nodes be removed. Rejected changes back off exponentially per keyspace (1 s ... 60 s). Nothing is scheduled while tablet load balancing is disabled (`tablets().balancing_enabled()`), so `disable_tablet_balancing` also freezes auto-RF. A manual `ALTER` which stays within the eligible racks and above the goal is kept; one which leaves an eligible rack out or adds an ineligible one is undone. `system.topology.needs_auto_rf_change` marks that a change is pending, which preempts tablet load balancing; the quiesce topology request schedules and waits out pending changes; decommission and removenode give the reconciler a bounded time to catch up before rejecting a removal as RF-rack-invalid. Consequences: a manual `ALTER KEYSPACE` on these keyspaces is reconciled away, and a cluster whose only tablets keyspaces are these two keeps them on a single rack.

Known limitation: a numeric RF in any user tablets keyspace makes *every* rack of that DC eligible (`get_eligible_racks_for_auto_rf_change` marks the DC as "all racks"), and the auto-RF keyspaces are converted to rack lists (the first racks of the eligible set) which are then never given up. In such a cluster, the shipped default (`rf_rack_valid_keyspaces: false`, `enforce_rack_list: false`), decommissioning or removing all nodes of a rack is blocked while an auto-RF keyspace lists it: the RF-rack validity check rejects it, or the drain of the last node fails with "No candidate nodes in <dc>/<rack>". On vnodes (before this feature) the operation worked. The workaround is to disable tablet load balancing (which pauses the reconciler; node drains still run), drop the rack from both keyspaces by hand, remove the nodes, then re-enable balancing. A fix would keep the auto-RF keyspaces numeric in DCs where every eligible keyspace is numeric, or treat racks whose token owners are all leaving as ineligible.
