# Hudi Internal — Claude Code Guide

## Project Overview

`hudi-internal` is Onehouse's internal mirror of Apache Hudi (`org.apache.hudi:hudi`, version `0.14.1-rc2`). It serves as a staging repo for internal fixes and enhancements before they are upstreamed to Apache Hudi OSS. All proprietary code belongs in `onehouse-dataplane`, not here.

**PR workflow**: Feature development goes through Hudi OSS (Apache) first, then is cherry-picked into hudi-internal. For urgent hotfixes, PRs are created with a placeholder OSS PR link and upstreamed later. PR titles must start with a ClickUp task ID: `[ENG-XXX]`, `[AUDIT-XXX]`, `[ONHS-XXX]`, `[CHERRYPICK]`, or `[BREAKGLASS]`.

### Key Modules

| Module | Description |
|--------|-------------|
| `hudi-common` | Core data structures, timeline metadata, schema management, exceptions |
| `hudi-client` | Write client implementations (sub-modules: client-common, java-client, spark-client, flink-client) |
| `hudi-spark-datasource` | Spark SQL DataFrame API and Datasource V2 (sub-modules: spark-common, spark) |
| `hudi-flink-datasource` | Flink DataStream source/sink connectors (per-Flink-version sub-modules) |
| `hudi-utilities` | Data ingestion tools — Kafka source, cloud object sources (S3/GCS), DFS sources |
| `hudi-sync` | Metadata sync with Hive, Datahub, ADB (sub-modules: sync-common, hive-sync, datahub-sync, adb-sync) |
| `hudi-cli` | Command-line tool for table inspection and operations |
| `hudi-kafka-connect` | Kafka Connect Sink connector |
| `hudi-aws` / `hudi-gcp` / `hudi-azure` | Cloud-specific implementations (S3, DynamoDB, GCS, Azure Blob) |
| `hudi-hadoop-mr` / `hudi-hadoop-common` | Hadoop MapReduce support and common Hadoop utilities |
| `hudi-timeline-service` | Timeline metadata service |
| `hudi-platform-service` | Platform services (contains hudi-metaserver) |
| `hudi-integ-test` | Docker-based integration tests |
| `packaging/` | 19+ distribution bundles (spark-bundle, flink-bundle, utilities-bundle, aws-bundle, etc.) |

**Tech stack**: Java 8, Scala 2.12, Maven, Apache Spark (default 3.4.1, supports 2.4–3.5), Apache Flink (default 1.17.1, supports 1.13–1.17), Parquet, Avro, Kafka, Hadoop 2.10.2, Hive 2.3.4, JUnit 5, Mockito

**Naming conventions**: use `hudi` in package names, `hoodie` in class names (mirrors OSS Hudi conventions).

## Development Setup

### Authentication

This repo pulls dependencies from AWS CodeArtifact. Authenticate before any build or test command:

```bash
code_artifact_auth
```

This alias is available from the [dev-tools](https://github.com/onehouseinc/dev-tools/blob/main/zsh/onehouse.plugin.zsh#L146) zsh plugin. Run it whenever your CodeArtifact token expires (~12 hours).

### Build

```bash
# Full build (skip tests)
mvn -T 14C clean package -DskipTests -Dscala-2.12 -Dspark3.5 -Dflink1.20

# Checkstyle
mvn checkstyle:check
mvn checkstyle:check -pl hudi-utilities
```

### Tests

**Before committing or pushing**, always run the relevant module tests to verify your changes.

Spark-based tests require two environment settings:
- `SPARK_LOCAL_IP=127.0.0.1` — required for Spark to bind correctly on macOS
- `-Dspark3.5` — activates the Spark 3.5 profile (needed to resolve `HoodieSparkSessionExtension` classes)

```bash
# Run tests for a specific module (most common workflow)
export SPARK_LOCAL_IP=127.0.0.1
mvn test -pl hudi-utilities -Dspark3.5 -Dtest="org.apache.hudi.utilities.sources.helpers.TestCloudObjectsSelectorCommon"

# Run a single test method
mvn test -pl hudi-utilities -Dspark3.5 -Dtest="org.apache.hudi.utilities.sources.helpers.TestCloudObjectsSelectorCommon#testGetObjectMetadataRepartitions"

# Unit tests (full suite)
mvn -Punit-tests test

# Functional tests
mvn -Pfunctional-tests test

# Integration tests (Docker required)
mvn -Pintegration-tests verify
```

## Coding Standards

Apply these standards when writing, reviewing, or suggesting changes to code in this repo.

### 1. Naming and Clarity

- Class, method, and variable names must be clear and unambiguous — flag overloaded or confusing names
- Names must match what the code actually does (e.g., `isFull()` should not check `closed` state)
- Never use `var` in Java — use explicit types
- Avoid redundant prefixes (e.g., don't use `hoodieTable` inside `HoodieTableManager` — just `table`)
- Config field names must not conflict (e.g., `topicWriterThreadPoolSize` vs `topicPartitionWriterThreadPoolSize`)
- Use consistent naming: `hudi` for package names, `hoodie` for class names in Hudi-related code
- Methods named "builder" must return a builder — flag misleading names (e.g., `getConnectorsUrlBuilder` returning `String`)
- Use domain-level names in generic classes — not technology-specific (e.g., `validateRelationalDataSourceConfig` not `validateDebeziumConfig`)
- Use consistent abbreviations — don't mix `DBZM` and `DEBEZIUM` in the same codebase
- Variable names like `ie`, `wr` have no context — use descriptive names even in lambdas
- Avoid names that conflict with framework or OSS Hudi classes (e.g., don't name a class `HoodieWriterConfig` when Hudi already has one)

### 2. Architecture and Design

- Single Responsibility Principle — flag god-classes
- Prefer interfaces over abstract classes when there's no shared implementation
- Prefer composition over inheritance
- Use factory methods and dependency injection — avoid hard-coding implementations (e.g., Glue-specific code in a generic handler)
- Program to interfaces, not implementations (`Queue` not `BlockingQueue` unless blocking semantics are needed)
- Handler → Service → DAO layering: handlers must not be aware of storage implementation details
- Don't use static methods for logic that needs to be tested/mocked
- Extract to a private method when indentation exceeds 2–3 levels
- Convert inline `CompletableFuture` lambdas to named methods
- Keep format-agnostic naming in interfaces — don't couple interface names to specific implementations (e.g., no "hudi" in generic LakeLog interfaces)
- Don't put custom code in third-party packages (e.g., `org.apache.spark.*`) — keep it in `com.onehouse.*` to distinguish from upstream
- Don't expose internal decisions to outer classes — each class manages its own context
- Avoid embedding side-effects in boolean checks
- Prefer type-safe enums over booleans for state that may have more than two values
- Config belongs in proper config classes, not as private fields in service classes
- Don't use `@Setter` on fields that require synchronization
- Use `@Singleton` on the class rather than empty constructors
- Extract common logic from duplicate methods into a shared private method
- Fetch data once per operation, not per-iteration
- Don't override methods that only call `super.x()` — let inheritance handle it

### 3. Error Handling

- Never wrap exceptions in generic `RuntimeException` — define meaningful domain exceptions (e.g., `ClosedBufferException`, `LimitExceededException`, `LakeLogException`)
- Don't return `null` for different failure modes — throw specific exceptions
- Fail fast: throw immediately on calls to a closed/invalid resource
- Use `IllegalArgumentException` for invalid inputs, not silent no-ops
- Don't catch broad exceptions when specific ones are expected
- Use `HoodieIOException` for IO errors in Hudi-related code
- Don't log and throw — do one or the other
- Don't swallow exception stacktraces in `String.format` — pass the exception as a separate logger arg
- Add max retry limits — trigger an alert for oncall intervention after repeated failures
- Use `Preconditions.checkArgument` (Guava) for input validation
- Use `NotSupportedOperationException` in implementations, not default interface methods
- Don't catch, wrap, and re-throw when you can let it propagate
- Combine catch blocks: `catch (HoodieIOException | IOException e)` instead of separate blocks
- Silent exits/skips must be classified as failures, not silently ignored

### 4. Memory and Performance

- Avoid unnecessary list copies — verify if `getRecords()` returns a new list or a reference
- Watch for memory references held during async operations (futures holding temp lists)
- Use `Collections.unmodifiableList()` for read-only returns
- Use `Collections.emptyList()` over `new ArrayList<>()` for empty returns
- Use `Collections.singletonMap()` for single-entry maps
- `ObjectMapper` is thread-safe and heavy — reuse, don't create per-record
- Use `anyMatch` instead of `filter().collect().size() > 0`
- Use lazy processing — don't load everything into memory
- Use `long` not `int` for sizes that could exceed 2GB
- Use `Math.floorMod` instead of `Math.abs()` for hash-based partitioning (abs can return negative for `Integer.MIN_VALUE`)
- Always specify an executor service for async operations — never use the default fork-join pool in production code
- Avoid unnecessary `Optional` wrappers — each is an extra allocation
- Avoid redundant `.stream().collect()` on collections that are already collections
- Use `Collectors.joining()` for comma-separated strings instead of manual `StringBuilder` loops
- Don't call expensive operations (e.g., `getAllPartitions`) when a cheaper alternative exists (e.g., checking latest commit's write paths)
- Process records lazily — avoid retaining unnecessary intermediate POJOs
- Set memory limits on Spark executors and drivers — the default is likely too high for small executors
- Account for daemonset overhead (logging, guard duty) when calculating executor counts

### 5. Testing Quality

- Tests MUST validate actual data, not just counts or types — assert the full output dataset
- Add failure mode and mixed success/failure test cases
- Use parameterized tests for boolean/enum variants
- Don't use `lenient()` in Mockito unless absolutely necessary — it weakens assertions
- Don't use reflection in tests — construct objects properly
- `.equals()` on `byte[]` always returns false in Java — use `Arrays.equals()` / `assertArrayEquals`
- Create expected objects and assert with `assertEquals` — catches regressions when new fields are added
- Test concurrent operations for classes with locks or shared state
- Critical path code MUST have tests
- Don't mock protos — build real objects (less verbose, more readable)
- Use `EnumSource` for parameterized tests driven by enum variants
- Use `verify(mock, timeout(N)).method()` for async assertions instead of sleep loops
- Prefer `map`/`flatMap` over `isPresent()` + `get()` on `Optional`
- Write functional tests with real data for critical paths — use existing test harnesses (e.g., `DataplaneSparkTestHarness`)
- Data pipeline changes MUST have data validation tests
- Sort test data on unique fields to avoid non-deterministic ordering
- Negative verification: verify wrong-path methods are NOT called with `verify(mock, never())`
- Validate timelines and state, not just return values — inspect the Hudi timeline where relevant
- When testing multiple tables, corrupt/fail only one to validate isolation
- Self-review AI-generated code before requesting review — "If you're using AI to write code, you NEED to self-review before asking for feedback"

### 6. Documentation and Javadocs

- Public methods that define a contract (lifecycle, side effects, usage) MUST have Javadocs
- Config fields MUST have Javadocs — critical for on-call debugging
- Don't add verbose line-by-line comments for obvious code
- Javadoc should describe WHAT and WHY, not implementation details
- Class-level Javadoc should match what the class actually is
- Explain magic numbers — define them as `static final` constants with meaningful names
- Update Javadocs when method contracts/parameters change — stale Javadocs are worse than none
- Flag contradictory Javadocs — when the doc says one thing but the code does another
- When two similar methods exist, add Javadoc clarifying the difference

### 7. Code Hygiene

- Use Lombok to reduce boilerplate: `@RequiredArgsConstructor`, `@AllArgsConstructor`, `@Value`, `@Builder`, `@Jacksonized`
- Use Java streams and collectors over manual loops where appropriate
- Define magic numbers/strings as `static final` constants
- Remove dead code, unused imports, and deprecated methods
- Remove `@SuppressWarnings` annotations
- Don't duplicate classes — consolidate (e.g., two `TopicPartition` classes)
- Use `toBuilder()` to modify proto objects instead of rebuilding from scratch
- Use Google Java Style (enforced via Spotless / checkstyle)
- Use `switch` consistently — don't mix `if-else` and `switch` in the same block
- Add `throw new UnsupportedOperationException()` in the `default` branch when switching on enums that may evolve
- Release locks in `finally` blocks
- Close resources: executor services, write clients, schema registry handlers, table handles
- Use `equalsIgnoreCase` for case-insensitive string comparisons
- Remove `@Builder` when using static factory methods — having both is confusing
- Avoid wildcard imports
- Don't pass `e.getMessage()` to a logger that also takes the exception — it's redundant
- Remove unused constructor parameters and injected fields
- Add `@Override` when implementing interface methods
- Move test certificates and large test data to `src/test/resources`, not inline strings

### 8. Resource Management and Lifecycle

- Resources opened must be closed — check `AutoCloseable` implementations
- Use try-with-resources for closeable resources
- Executor services must be shut down
- Use fixed thread pools, not `newCachedThreadPool()` (unless justified with a comment)
- Return `CompletableFuture` instead of blocking with `.get()` — use `thenApply`/`thenCompose`
- Check for double-close and double-complete scenarios on buffers/futures

### 9. Configuration

- Question every default value — document the reasoning
- Config property names must align with their field names
- Consolidate duplicate config classes
- Validate config values at system boundaries (e.g., `overheadFactor` must be between 0 and 1)
- Add feature flags through the proper system (product-config/idls), not ad-hoc booleans
- Prefer per-table config classes over Spark configs that require a driver restart to change
- Return sensible defaults from config accessors to avoid conditional checks at every call site
- Config naming must not be cloud-specific when the feature is cloud-agnostic (e.g., `region` not `awsRegion`)
- Check dependency licenses when adding new third-party SDKs — some licenses are not accepted by Apache

### 10. Operational Awareness

- Use DEBUG for noisy/verbose logs, WARN/ERROR for actionable issues
- Don't log at INFO level for per-record operations — it causes OpenSearch spikes
- Add alerts for critical failure scenarios (e.g., repeated restarts, operator failures)
- Consider backward compatibility for config and API changes
- Consider multi-cloud compatibility — don't hard-code to AWS/GCP specifics
- Don't use `latest` tags in Docker — pin versions
- Log the full exception including stack trace, not just `e.getMessage()`
- Include identifiers (table UUID, base path, org ID) in log messages for debugging
- Alert thresholds should be reasonable — "2 or more times in an hour" not every single occurrence
- Differentiate user failures from system failures with separate metrics/alerts
- Don't log at ERROR level per-item when processing collections — log once per batch
- Consider race conditions in distributed systems
- Use structured logging with consistent key/value pairs
- Docker CLI tools installed via `apt-get` should pin versions