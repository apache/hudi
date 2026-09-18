---
name: hudi-index
description: Choose and configure Hudi index types. Use when asking about bloom, bucket, record-level, or HBase indexes for upsert performance.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [question e.g. "which index for high cardinality", "bloom filter false positives", "bucket index sizing"]
---

# Hudi Index Selection Guide

Question: **$ARGUMENTS**

## Instructions

### Key source files:
- Index config: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieIndexConfig.java`
- Index types: search for `HoodieIndex.IndexType` enum
- Bloom index: search for `HoodieBloomIndex`, `BloomFilter` classes
- Bucket index: search for `BucketIndex` classes
- Record-level index: search for `RecordIndex`, `RECORD_INDEX` in metadata table
- Global vs non-global: search for `isGlobal()` in index implementations

### Index types to explain:

**1. Bloom Index** (default for CoW)
- Per-file bloom filters in Parquet footer
- Good for: append-mostly with ordered keys
- Gotchas: false positives with random keys, expensive for global lookups
- Key configs: `hoodie.bloom.index.filter.type`, `hoodie.bloom.index.num.entries`, `hoodie.index.bloom.fpp`

**2. Simple Index**
- Brute force lookup (reads all files to find records)
- Good for: small tables, testing
- Not for production large-scale use

**3. Bucket Index**
- Hash-based partitioning into fixed buckets (file groups)
- Good for: known data distribution, high update rate
- Gotchas: bucket count is fixed at creation, wrong bucket count = poor performance
- Key configs: `hoodie.bucket.index.num.buckets`, `hoodie.bucket.index.hash.field`

**4. Record-Level Index** (metadata table based)
- Exact record-to-file mapping stored in metadata table
- Good for: random key patterns, high cardinality, global index needs
- Requires metadata table enabled
- Config: `hoodie.metadata.record.index.enable`

**5. HBase Index**
- External HBase for index storage
- Good for: very large tables, global index at scale
- Operational overhead of managing HBase

### For each index type, analyze:
1. **Write performance** - lookup cost during upsert
2. **Read performance** - does the index help reads?
3. **Scalability** - how it behaves as table grows
4. **Global vs non-global** - can it find records across partitions?
5. **Memory requirements** - what's held in memory during operations

### Output:
1. **Recommendation** for the user's workload
2. **Config template** for the recommended index
3. **Migration path** if switching index types on existing table
4. **Monitoring** - how to detect index performance issues
