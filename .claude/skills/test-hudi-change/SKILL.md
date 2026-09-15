---
name: test-hudi-change
description: Write tests for Hudi changes following project guidelines. Use when writing unit, functional, or integration tests for Hudi code.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [what to test e.g. "compaction with empty partitions", "schema evolution for nested fields"]
---

# Write Hudi Tests

Test target: **$ARGUMENTS**

## Instructions

### Testing guidelines (from CLAUDE.md):
- Use given-when-then model
- Split to new test cases when appropriate
- Use plain for-loops to avoid try-catch in lambdas
- Use static imports for constants and helpers
- Avoid reusing local variable names
- Keep helpers local until obviously generic

### Test utilities to use:
Search the codebase for these test utilities:
- `HoodieTestTable` - Generate test tables with empty files (path manipulation)
- `HoodieWriteableTestTable` - Generate test tables with actual records
- `HoodieTestDataGenerator` - Generate test records
- `HoodieClientTestBase` / `HoodieClientTestHarness` - Base test class for client tests
- `SparkClientFunctionalTestHarness` - Spark functional test base
- `Assertions` (custom) - Hudi assertion helpers
- `SchemaTestUtil` - Schema test helpers
- Don't put new helpers in `HoodieTestUtils` or `HoodieClientTestUtils` (too generic)

### Test categories:
1. **Unit tests** - Class-level, may use mocks. Fast.
2. **Functional tests** - Brings up services, no mocking. Uses `@Tag("functional")`
3. **Integration tests** - Full environment with Docker services

### Steps:
1. Find existing tests for the area being changed (search for test classes in the relevant module's `src/test/`)
2. Identify the right base class to extend
3. Write tests following given-when-then:
   - **Given**: Setup test data, table, configs
   - **When**: Execute the operation
   - **Then**: Assert expected state
4. Test these edge cases:
   - Null/empty inputs
   - Single vs multi partition
   - CoW and MoR (if applicable)
   - Empty table / first write
   - Concurrent operations (if applicable)
   - Schema with nested fields (if schema-related)

### Test naming:
Use descriptive names: `testCompactionWithEmptyPartitions()` not `test1()`
