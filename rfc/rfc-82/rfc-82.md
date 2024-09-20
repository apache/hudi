<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# RFC-82: Concurrency control of schema evolution

## Proposers

- @jonvex
- @Davis-Zhang-Onehouse

## Approvers
- @yihua
- @sivabalan

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-8221

## Abstract

This RFC proposes to enhance Hudi's optimistic concurrency control mechanism to handle concurrent schema evolution scenarios. The current implementation does not adequately address conflicts that may arise when multiple transactions attempt to alter the table schema simultaneously. This enhancement aims to detect and prevent such conflicts, ensuring data consistency and integrity in multi-writer environments.

## Background

Hudi supports optimistic concurrency control to handle concurrent write operations. However, the existing implementation does not specifically account for schema evolution conflicts. In a multi-writer environment, it's possible for different clients to attempt schema changes concurrently, which can lead to inconsistencies if not properly managed.

Schema evolution is a critical operation that can significantly impact data compatibility and query results. Uncontrolled concurrent schema changes can result in data inconsistencies, failed reads, or incorrect query results. By extending the concurrency control mechanism to handle schema evolution, we can prevent these issues and ensure a more robust and reliable data management system.

| Scenario | Schema when Txn Start | Schema when Txn Validates | Schema Used by curr Txn | Should Conflict? | Schema used by Commit Metadata of curr Txn | Notes |
|----------|---------------------|--------------------------|-------------------|------------------|---------------------------------------|-------|
| 1 | Not exists | Not exists | S1 | No | S1 | Current txn is the first commit ever, conflict is impossible |
| 2 | Not exists | S1 | S1 | No | S1 | Second commit, no schema evolution |
| 3 | Not exists | S1 | S2 | Yes | N/A (throws exception) | No predefined schema, effectively concurrent schema definition |
| 4 | S1 | S1 | S1 | No | S1 | No schema evolution |
| 5 | S1 | S1 | S2 | No | S2 | Schema evolution in current transaction |
| 6 | S1 | S2 | S1 | No | S2 | Backwards compatibility handles it |
| 7 | S1 | S2 | S2 | No | S2 | Concurrent evolution to same schema |
| 8 | S1 | S2 | S3 | Yes | N/A (throws exception) | Concurrent evolution to different schemas |

Notes:
- S1, S2, S3 represent different schemas
- 3 schemas to consider:
  + The table schema from the last committed txn when the current txn starts.
  + The table schema from the last committed txn when the current txn validates.
  + The table schema that the current txn uses.
- "Not exists" means there were no completed transactions at that point

Key observations:
1. Conflicts are avoided when schemas are the same or when backwards compatibility can handle the changes.
2. The first commit to an empty table is always allowed, but concurrent first commits with different schemas will conflict.
3. Conflicts occur when there are concurrent, incompatible schema changes. the compatibility exmaination is kept naive - as long as S2 and S3 are not the same, they are considered as a conflict. In the future we can think of come up with intellegent algorithm to further resolve the conflicts for better concurrency.

## Implementation

The proposed implementation involves the following key changes:

1. Enhance the `TransactionUtils` class to include schema conflict detection:
   - Add a new method `abortTxnOnConcurrentSchemaEvolution` to check for schema conflicts.

2. Schema conflict detection logic:
   - Follow the graph as explained above.

## Rollout/Adoption Plan

1. Impact on existing users:
   - Users leveraging multi-writer capabilities with schema evolution will benefit from improved consistency.
   - No breaking changes for users not using concurrent schema evolution.

2. Phasing out older behavior:
   - The code is not protected by a feature flag, once it is merged the behavior is adopted in the next release.

3. Migration:
   - No special migration tools are required.
   - Users should update to the latest Hudi version to benefit from this enhancement.


## Test Plan

The implementation includes comprehensive test cases in the `TestHoodieClientMultiWriter`:

1. Test scenarios:
   - Covered by the table in the previous section.

2. Test variations:
   - Both COPY_ON_WRITE and MERGE_ON_READ table types
