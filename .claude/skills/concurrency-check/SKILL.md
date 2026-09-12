---
name: concurrency-check
description: Review Hudi multi-writer and locking config. Use when dealing with write conflicts, lock timeouts, OCC, or concurrent writers.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [scenario e.g. "multi-writer setup", "lock timeout", "write conflict", "OCC"]
---

# Hudi Concurrency Control Check

Scenario: **$ARGUMENTS**

## Instructions

### Key source files:
- Lock config: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieLockConfig.java`
- Write config concurrency: search for `WRITE_CONCURRENCY_MODE` in `HoodieWriteConfig.java`
- Lock providers: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/lock/`
- Conflict resolution: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/`
- Early conflict detection: search for `EarlyConflictDetection` classes

### Concurrency modes:
1. **SINGLE_WRITER** (default) - No locking needed, single writer assumed
2. **OPTIMISTIC_CONCURRENCY_CONTROL** - Multiple writers, OCC with conflict detection at commit time

### For multi-writer setup review:
1. Check these configs are set correctly:
   - `hoodie.write.concurrency.mode=optimistic_concurrency_control`
   - `hoodie.write.lock.provider` - Must be set (not filesystem-based for production)
   - Lock timeout and retry configs
2. Recommended lock providers by environment:
   - **ZooKeeper**: `org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider`
   - **DynamoDB**: `org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider`
   - **HiveMetastore**: `org.apache.hudi.hive.transaction.lock.HiveMetastoreBasedLockProvider`
   - **Storage-based**: Only for testing, NOT production multi-writer
3. Early conflict detection: `hoodie.write.concurrency.early.conflict.detection.enable`

### For write conflict debugging:
1. Check the `HoodieWriteConflictException` stack trace
2. Identify which file groups are in conflict
3. Check if both writers were modifying the same partition
4. Review conflict resolution strategy: `hoodie.write.concurrency.conflict.resolution.strategy`

### For lock timeout issues:
1. Check `hoodie.write.lock.wait_time_ms` (default varies by provider)
2. Check `hoodie.write.lock.num_retries`
3. Check if long-running table services hold locks
4. Heartbeat config: `hoodie.write.lock.heartbeat_interval_ms`

### Critical rules from code review guidelines:
- Lock must be held when requesting AND completing timeline actions
- Timeline must be refreshed inside lock's critical section
- Locks must be released properly (check finally blocks)
- Non-atomic check-then-act on distributed storage is a bug

### Output:
1. **Current config assessment** - what's configured and is it correct
2. **Issues found** - misconfigurations or risks
3. **Recommended config** - complete config block for the scenario
4. **Testing approach** - how to validate multi-writer behavior
