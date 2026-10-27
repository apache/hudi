---
name: schema-evolution
description: Guide Hudi schema changes. Use when adding, dropping, renaming columns, changing types, or hitting schema compatibility errors.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [operation e.g. "add column", "rename column", "change type", "schema compatibility error"]
---

# Hudi Schema Evolution Guide

Operation: **$ARGUMENTS**

## Instructions

### Key source files:
- Schema evolution internal model: `hudi-common/src/main/java/org/apache/hudi/internal/schema/`
- Table changes: `hudi-common/src/main/java/org/apache/hudi/internal/schema/action/TableChanges.java`
- Schema compatibility: `hudi-common/src/main/java/org/apache/hudi/internal/schema/utils/SchemaChangeUtils.java`
- Avro schema utils: `hudi-common/src/main/java/org/apache/hudi/avro/HoodieAvroUtils.java`
- Spark schema resolution: search for `schema evolution` in `hudi-spark-datasource/`

### Supported operations:
1. **Add column** - Add new column at any position (top-level or nested)
2. **Drop column** - Remove column (data preserved in existing files)
3. **Rename column** - Rename column (tracked by internal schema ID)
4. **Type promotion** - Widen types (int->long, float->double, etc.)
5. **Reorder columns** - Change column position

### For each operation, explain:
1. **Spark SQL syntax**: `ALTER TABLE ... ADD/DROP/RENAME COLUMNS`
2. **DataFrame option**: schema reconciliation configs
3. **What happens internally** - how the schema change is recorded
4. **Backward compatibility** - can old readers still read the data?
5. **Forward compatibility** - can new readers read old files?
6. **Gotchas** - common mistakes and limitations

### Key configs:
- `hoodie.schema.on.read.enable` - Enable schema-on-read (schema evolution)
- `hoodie.avro.schema.validate` - Validate schema compatibility on write
- `hoodie.datasource.write.reconcile.schema` - Auto-reconcile schema differences

### Common issues:
- **Type narrowing rejected**: Hudi prevents narrowing (long->int) to avoid data loss
- **Nested field evolution**: Supported but has more edge cases
- **Schema with metadata columns**: Cannot reorder/rename Hudi metadata fields
- **Partition column evolution**: Limited - partition columns have special constraints

### Schema compatibility rules:
Read `TableChanges.java` for the validation logic. Key rules:
- Cannot add column if parent doesn't exist
- Cannot add same column twice
- Cannot change fixed-size types
- Cannot move columns between meta and non-meta positions
- Recursive record types not supported

### Output:
1. **How to do it** - exact SQL/API syntax
2. **What happens** - internal mechanics
3. **Risks** - what could go wrong
4. **Verification** - how to confirm the change worked correctly
