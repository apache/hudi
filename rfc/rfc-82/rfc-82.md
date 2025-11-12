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
- @danny0405

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-8221

## Abstract

This RFC proposes to enhance Hudi's concurrency control mechanism to handle concurrent schema evolution scenarios. The current implementation does not adequately address conflicts that may arise when multiple transactions attempt to alter the table schema simultaneously. This enhancement aims to detect and prevent such conflicts, ensuring data consistency and integrity in multi-writer environments.

## Background

### Hudi Schema
In Apache Hudi, schema management is a critical component that ensures data consistency and facilitates efficient data processing, especially in environments where data structures may evolve over time.

The schema of the input data represents the structure of incoming records being ingested into Hudi from various sources. During a write operation, the Hudi write client utilizes a **writer schema**, which is typically derived from the input data schema or specified by a schema provider. This **writer schema** is applied throughout the transaction to maintain consistency.

Additionally, Hudi stores the **table schema** within the commit metadata on storage, capturing the authoritative schema of the hudi table table at the time of each commit. This stored schema is crucial for readers to correctly interpret the data and for managing schema evolution across different data versions. These schemas are determined through a reconciliation process that considers both the incoming data schema and the existing **table schema**, allowing Hudi to handle schema changes gracefully. Currently, Hudi uses this schema management approach to enable seamless read and write operations, support upserts and deletes, and manage schema evolution, ensuring that the system remains robust even as the underlying data structures change.

### Concurrency control over concurrent schema evolution
Hudi supports concurrency control to handle concurrent write operations. However, the existing implementation does not specifically account for schema evolution conflicts. In a multi-writer environment, it's possible for different clients to attempt schema changes concurrently, which can lead to inconsistencies if not properly managed.

Schema evolution is a critical operation that can significantly impact data compatibility and query results. Uncontrolled concurrent schema changes can result in data inconsistencies, failed reads, or incorrect query results. By extending the concurrency control mechanism to handle schema evolution, we can prevent these issues and ensure a more robust and reliable data management system.

## Design

Depending on whether there are concurrent schema evolution transactions, an inflight transaction may see a different latest schema of the table when it checks at different time, as there can always be other transactions committed and potentially changed schema as a result of that in multi-writer scenarios.

We use "txn" as a abbreviation of transaction.

| Scenario | Table Schema when Txn Start | Table Schema when Txn Validates | Write Schema Used by curr Txn | Should Conflict? | New Table Schema in the Commit Metadata of curr Txn | Notes |
|----------|---------------------|--------------------------|-------------------|------------------|---------------------------------------|-------|
| 1 | Not exists | Not exists | S1 | No | S1 | Current txn is the first commit ever, conflict is impossible |
| 2 | Not exists | S1 | S1 | No | S1 | Second commit, no schema evolution |
| 3 | Not exists | S2 | S3 | Yes | N/A (throws exception) | No predefined schema, effectively concurrent schema definition. The design decision made here is to identify this as schema conflict for simplicity. It can also be broken down into cases where (1) S3 is evolvable from S2, (2) S3 is not evolvable from S2, for further optimization. |
| 4 | S1 | S1 | S1 | No | S1 | No schema evolution |
| 5 | S1 | S1 | S2 | No | S2 | Schema evolution in current transaction |
| 6 | S1 | S2 | S1 | No | S2 | Backwards compatibility handles it |
| 7 | S1 | S2 | S2 | No | S2 | Concurrent evolution to same schema |
| 8 | S1 | S2 | S3 | Yes | N/A (throws exception) | Concurrent evolution to different schemas. Same as case 2, for future optimization we should consider evolvibility from S2 to S3 |

For timeline graph of each case please refer appendix.

Notes:
- S1, S2, S3 represent different schemas. The proposed solution does not introduce any new assumptions on compatibility among those schemas. But if "Table Schema when Txn Start" is schema X and Table Schema when Txn Validates is Y, it naturally means Y is compatible and is evolved from X as it is guaranteed by today's implementation.
- 3 schemas to consider:
  + The **table schema** from the last committed txn when the current txn starts.
  + The **table schema** from the last committed txn when the current txn validates.
  + The **writer schema** that the current txn uses.
- "Not exists" means the table is empty and **table schema** is not set.

## Implementation

The proposed implementation involves the following key changes:

1. Enhance the `TransactionUtils` class to include schema conflict detection:
   - Add a new interface `SchemaConflictResolutionStrategy` to check for schema conflicts.
   - Add an implementation to this new interface `SimpleSchemaConflictResolutionStrategy` which implements the table above.

2. Schema conflict detection logic:
   - Follow the graph as explained above.

Pseudo code:
```
// Initialize schemas
writerSchemaOfTxn = writer schema of the current transaction
tableSchemaAtTxnStart = table schema at transaction start (if available)
tableSchemaAtTxnValidation = table schema at transaction validation (if available)

// Case 1: First commit ever
if tableSchemaAtTxnValidation is null:
  return writerSchemaOfTxn

// Case 2, 3: Second commit, one commit is done concurrently after this commit has started
if tableSchemaAtTxnStart is null:
  if writerSchemaOfTxn != tableSchemaAtTxnValidation:
    throw ConcurrentSchemaEvolutionError
  return writerSchemaOfTxn

// Table schema does not change at the pre-commit validation compared to the table schema at the start of the transaction
// Compatible case 4,5
if tableSchemaAtTxnStart == tableSchemaAtTxnValidation:
  return writerSchemaOfTxn

// Table schema has changed from the start of the transaction till the pre-commit validation

// Compatible case 7
if writerSchemaOfTxn == tableSchemaAtTxnValidation:
  return writerSchemaOfTxn

// Writer schema is different from the table schema at the pre-commit validation
// Compatible case 6
if writerSchemaOfTxn == tableSchemaAtTxnStart:
  return tableSchemaAtTxnValidation

// Case 8: Multiple commits, potential concurrent schema evolution
throw ConcurrentSchemaEvolutionError
```

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

# Appendix

## Q&A
### How does spark executor know what is the **writer schema** to use?
It is populated from the writer config. In org.apache.hudi.io.HoodieWriteHandle we have the following code
```
  protected HoodieWriteHandle(HoodieWriteConfig config, String instantTime, String partitionPath, String fileId,
                              HoodieTable<T, I, K, O> hoodieTable, Option<Schema> overriddenSchema,
                              TaskContextSupplier taskContextSupplier) {
    super(config, Option.of(instantTime), hoodieTable);
    this.partitionPath = partitionPath;
    this.fileId = fileId;
    this.writeSchema = overriddenSchema.orElseGet(() -> getWriteSchema(config));
    this.writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.allowOperationMetadataField());
   ...
  }
```
So all executors from the same writer use the same schema of the writer. We don't need to worry about as latest table schema changes, somehow these executors will use different configs while writing data. It is safe to exclude executors from the RFC design.

### Do we really need to factor in Table Schema when Txn Start for schema resolution?
Q1: In case 3 where "Table Schema when Txn Start" is None, "Table Schema when Txn Validates"(S_REAL_AT_V) is S1 and "Schema Used by curr Txn"(S_OF_TXN) is S2, we only need the later 2 factor to complete the resolution - we abort as long as the 2 are not the same. Similarly, for case 5,6,7,8 can we just compare Table Schema when Txn Validates & Schema Used by curr Txn to complete the resolution?


A: Yes we can but it is error-prone. For example, we can do follows:
```
if (cannot evolve from S_REAL_AT_V to S_OF_TXN) {
   throw ConflictSchemaEvolutionError
}

use S_REAL_AT_V or S_OF_TXN, whichever is more "general".
```

For the "general" comparison, one simple case is we find S_OF_TXN has 1 more column than S_REAL_AT_V, so we use S_OF_TXN in commit metadata (This is case 5). If it is in a reverse way that S_REAL_AT_V has 1 more column then it is case 6. If cannot evolve from S_REAL_AT_V to S_OF_TXN then it is case 8, for example, both 2 schema has columns that is absent from each other.

Here we need to look into the content of the schema to make a call, complexity grows if there are nested columns. The proposed solution only needs equality check, which is simpler.

In conclusion, it is doable to get rid of "Table Schema when Txn Start", we choose not to pay for that.

## Timeline of concurrency schema evolution case

**Scenario 1**


Should Conflict?: No
```
Time ---------------------------------------------------------------------------------->

Txn1:    [ Start Txn1 ]------------------------------[ Validate ]--------------------[ Commit Txn1 ]
      Uses **writer schema**: S1                                          Writes table schema in commit metadata: S1
Table Schema:
          [ Not Exists ]----------------------------[ Not Exists ]--------------------[ S1 ]
```
Notes:
- Txn1 is the first transaction; it creates the table with **table schema** S1.
- No conflicts occur since there are no other transactions.


**Scenario 2**

```
Time ---------------------------------------------------------------------------------->

Txn1:    [ Start Txn1 ]------------------------[ Commit Txn1 ]
                                             Creates Table Schema: S1

Txn2:            [ Start Txn2 ]------------------------[ Validate ]--------------------[ Commit Txn2 ]
            Uses Writer Schema: S1                                          Writes Table Schema in commit metadata: S1

Table Schema:
          [ Not Exists ]-----------------------[ S1 ]----------------------------------[ S1 ]
```
Notes:
- Txn2 starts when the table does not exist.
- Txn1 commits before Txn2 validates, creating **table schema** S1.
- Txn2 validates against **table schema** S1; no conflict occurs.

**Scenario 3**
```
Time ---------------------------------------------------------------------------------->

Txn1:    [ Start Txn1 ]------------------------[ Commit Txn1 ]
      Creates Writer Schema: S1

Txn2:            [ Start Txn2 ]------------------------[ Validate ]----X
               Uses Writer Schema: S2

Table Schema:
          [ Not Exists ]-----------------------[ S1 ]----------------------------------------
```
Notes:
- Txn2 starts when the table does not exist, intending to use **writer schema** S2.
- Txn1 commits before Txn2 validates, creating **table schema** S1.
- At validation, Txn2 detects schema conflict (S1 vs. S2); transaction fails.

A future improvement is to check the compatibility between S1 and S2 and reconcile properly.

**Scenario 4**
```
Time ---------------------------------------------------------------------------------->

Txn1:    [ Start Txn1 ]------------------------------[ Validate ]--------------------[ Commit Txn1 ]
      Uses Writer Schema: S1                                         Writes Table Schema in commit metadata: S1

Table Schema:
          [ S1 ]------------------------------------[ S1 ]----------------------------[ S1 ]
```
Notes:
- Txn1 operates entirely under **table schema** S1 (there is no concurrent writer or the concurrent writer does not evolve the **table schema**).
- No schema changes occur; no conflicts arise.

**Scenario 5**
```
Time ---------------------------------------------------------------------------------->

Txn1:    [ Start Txn1 ]------------------------------[ Validate ]--------------------[ Commit Txn1 ]
      Uses Writer Schema: S2                                                  Writes Table Schema in commit metadata: S2

Table Schema:
          [ S1 ]------------------------------------[ S1 ]----------------------------[ S2 ]
```
Notes:
- Txn1 starts with **writer schema** S1 and evolves the **table schema** to S2 within the transaction.
- No other transactions interfere; no conflicts occur.

**Scenario 6**
```
Time ---------------------------------------------------------------------------------->

Txn2:           [ Start Txn2 ]------------------------[ Commit Txn2 ]
                   Evolves Table Schema: S1 to S2

Txn1:    [ Start Txn1 ]------------------------------[ Validate ]--------------------[ Commit Txn1 ]
      Uses Writer Schema: S1                                                 Writes Table Schema in commit metadata: S2

Table Schema:
          [ S1 ]---------------------------------------[ S2 ]---------------------------------[ S2 ]
```
Notes:
- Txn1 starts with **writer schema** S1.
- Txn2 commits before Txn1 validates, evolving the **table schema** to S2.
- Txn1 validates against **table schema** S2; backward compatibility allows it to proceed.
- Txn1 writes data compatible with S2; commits successfully with the **table schema** S2, instead of the **writer schema** S1.

**Scenario 7**
```
Time ---------------------------------------------------------------------------------->

Txn2:           [ Start Txn2 ]------------------------[ Commit Txn2 ]
            Evolves Table Schema: S1 to S2

Txn1:    [ Start Txn1 ]------------------------------[ Validate ]--------------------[ Commit Txn1 ]
      Uses Writer Schema: S2                                                  Writes Table Schema in commit metadata: S2

Table Schema:
          [ S1 ]--------------------------[ S2 ]----------------------------------------[ S2 ]
```
Notes:
- Both Txn1 and Txn2 aim to evolve **table schema** from S1 to S2.
- Txn2 commits before Txn1 validates, updating **table schema** to S2.
- Txn1 detects that the **table schema** is already S2; no conflict occurs.
- Txn1 commits successfully.

**Scenario 8**
```
Time ---------------------------------------------------------------------------------->

Txn2:           [ Start Txn2 ]------------------------[ Commit Txn2 ]
            Evolves Table Schema: S1 to S2

Txn1:    [ Start Txn1 ]------------------------------[ Validate ]----X
      Uses Writer Schema: S3

Table Schema:
          [ S1 ]--------------------------------------[ S2 ]-------------------------------------[S2]
```
Notes:
- Txn1 intends to evolve **table schema** from S1 to S3.
- Txn2 commits before Txn1 validates, updating **table schema** to S2.
- At validation, Txn1 detects schema conflict (S2 vs. S3); transaction fails.

A future improvement is to check the compatibility of S2 and S3 trying to reconcile properly.
