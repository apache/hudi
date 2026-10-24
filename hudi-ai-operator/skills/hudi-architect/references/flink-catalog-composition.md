<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Flink catalog and metastore composition

Use this reference only after the user asks for named catalog resolution or for an external engine
to discover the table. Catalog composition is orthogonal to design contract schemas 1, 2, and 3;
it does not create a schema 4 or relax any PR2–PR5 safety invariant.

The baseline is Hudi 1.2.0 at `f05c83f2b97732de7a558ff9b26959e1139c05f5`, Flink 1.20,
and the Flink 1.20.1 fixture. Validate the checked-in manifest before using this reference.

## Supported profiles

| Profile | Table resolution | External discovery | Sync owner | Data lifecycle owner |
|---|---|---|---|---|
| `direct_sql_none` | Direct Flink SQL | None | None | Application |
| `direct_sql_hive_metastore` | Direct Flink SQL | Hive Metastore | Flink sink | Application |
| `hudi_dfs_catalog_none` | Hudi DFS Catalog | None | None | Catalog |
| `hudi_dfs_catalog_hive_metastore` | Hudi DFS Catalog | Hive Metastore | Flink sink | Catalog |
| `hudi_hms_catalog_hive_metastore` | Hudi HMS Catalog | Hive Metastore | Hudi HMS Catalog | Application |

The legacy contract remains `direct_sql_none` when `safety.external_catalog=false` and no
`catalog` block is supplied. If that gate is true, require an explicit block. A gate/profile or
owner mismatch is a conflict, not a request for the validator to choose an architecture.

AWS Glue, HMS without external discovery, managed HMS tables, unknown modes, multi-writer
operation, existing-table catalog mutation, and unconfirmed DFS lifecycle are not executable.

## Catalog contract

Add an optional top-level block to an existing schema 1, 2, or 3 design:

```json
{
  "catalog": {
    "contract_schema": 1,
    "resolution_mode": "hudi_hms_catalog",
    "external_discovery": "hive_metastore",
    "sync_owner": "hudi_hms_catalog",
    "data_lifecycle_owner": "application",
    "catalog_name": "production_hudi",
    "database": "analytics",
    "hive_configuration": {
      "source": "hive_conf_dir",
      "location": "/etc/hive/conf"
    },
    "table_external": true,
    "mor_sync_strategy": "ALL"
  }
}
```

- `catalog_name` is required for DFS and HMS Catalog and forbidden for direct SQL.
- `database` is required for catalog resolution or Hive discovery.
- `catalog_path` is required only for DFS Catalog.
- `hive_configuration` is required for Hive discovery and forbidden otherwise.
- Direct/DFS sink-owned sync accepts credential-free `thrift://` `metastore_uris`.
- HMS Catalog accepts an absolute `hive_conf_dir`, or `classpath` with no location. Embedded HMS
  is not supported.
- `table_external=true` is mandatory for HMS Catalog. The application owns its data lifecycle.
- `mor_sync_strategy` applies only to schema 3 plus Hive discovery and is derived from query modes.

Do not duplicate the table name, table path, table type, schema, or partition fields in this block.
They are derived from the existing desired contract.

## Path and lifecycle rules

Direct SQL keeps the explicit desired table path. DFS Catalog derives
`${catalog.path}/${database}/${table.name}`; the desired path must match exactly, and the table DDL
must not contain a misleading `path` option. The caller must explicitly accept catalog-owned data:
Hudi 1.2.0 DFS Catalog recursively deletes data during a table drop. Architect never emits DROP.

HMS Catalog must render the desired path in table DDL and must not fall back to a warehouse path.
Only `table.external=true` is supported so catalog removal does not imply catalog-owned data.

## Hive sync and MOR naming

For direct SQL or DFS Catalog with Hive discovery, the Flink sink owns these table options:

- `hive_sync.enabled=true`
- `hive_sync.mode=hms`
- `hive_sync.metastore.uris`
- `hive_sync.db`
- `hive_sync.table`

HMS Catalog supplies those values from catalog context; do not duplicate them in table DDL.
Validate the effective Hudi 1.2.0 defaults in both cases: ignore exceptions false, timestamp support
true, RO suffix skipping false, and snapshot-with-base-name true. Partition fields come from the
existing table contract.

For MOR plus Hive discovery, derive the fixed table strategy and registration names:

| Query requirement | Strategy | Registered names |
|---|---|---|
| Snapshot only | `RT` | base name |
| Read optimized only | `RO` | base name |
| Both | `ALL` | `base_ro`, `base_rt`, and base name |

Custom suffix behavior, custom names, or a different snapshot-with-base-name default requires
review. Never infer those changes from external-engine preferences.

## Freshness and deployment boundary

Hive sync runs asynchronously after a successful checkpoint commit. Record catalog freshness as
`post_commit_eventual`; do not claim atomicity with the Hudi commit or an external-visibility SLA.
Keep commit freshness, Hudi query freshness, and catalog freshness separate.

`catalog_deployment_checks` are operator checks, not static validation failures. Depending on the
profile they cover bundle dependencies, DFS path existence and permissions, Hive configuration,
HMS connectivity, database/table/partition registration, MOR names, and external-engine
visibility. The validator generates SQL but never executes it or probes a live service.

## Existing tables and secrets

An existing-table desired contract may contain a catalog block, but PR6 does not register, alter,
rename, delete, or take over an existing catalog entry. Any non-legacy catalog composition is
`FLINK_EXISTING_CATALOG_MUTATION_UNVERIFIED`, remains non-executable, and has an `UNVERIFIED`
reconciliation and change disposition.

Never request or render usernames, passwords, tokens, IAM material, keytabs, private keys, or
credential-bearing URIs. Reference credential-free deployment configuration only; do not read or
echo `hive-site.xml` contents.
