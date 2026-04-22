# Hudi Trino Plugin

A Trino connector for querying Apache Hudi tables. Supports both Copy-On-Write (COW) and Merge-On-Read (MOR) table types with snapshot, read-optimized, and real-time query modes.

## Compatibility

| Component | Version |
|-----------|---------|
| Trino     | 480     |
| Hudi      | 1.0.2   |
| JDK       | 25+     |

## Building

Requires JDK 25 (Trino 480's requirement).

```bash
cd hudi-trino-plugin
mvn clean compile jar:jar -DskipTests
```

The plugin jar is produced at `target/trino-hudi-480.jar`.

**Building with JDK 24** (with version warnings):

```bash
mvn clean compile jar:jar -DskipTests \
  -Denforcer.skip=true \
  -Dair.check.skip-all=true \
  -Dmaven.compiler.release=24 \
  -Dair.compiler.fail-warnings=false \
  -Dproject.build.targetJdk=24
```

## Deploying to Trino 480

### Option 1: Replace jars in stock Trino installation

Trino 480 ships with a stock Hudi plugin at `$TRINO_HOME/plugin/hudi/`. Replace the following jars:

```bash
PLUGIN_DIR=$TRINO_HOME/plugin/hudi

# Remove stock jars
rm -f $PLUGIN_DIR/io.trino_trino-hudi-480.jar
rm -f $PLUGIN_DIR/org.apache.hudi_hudi-common-1.1.1.jar
rm -f $PLUGIN_DIR/org.apache.hudi_hudi-io-1.1.1.jar

# Copy custom plugin jar
cp target/trino-hudi-480.jar $PLUGIN_DIR/io.trino_trino-hudi-480.jar

# Copy Hudi 1.0.2 jars (from local Maven repository)
M2=~/.m2/repository
cp $M2/org/apache/hudi/hudi-common/1.0.2/hudi-common-1.0.2.jar         $PLUGIN_DIR/
cp $M2/org/apache/hudi/hudi-io/1.0.2/hudi-io-1.0.2.jar                 $PLUGIN_DIR/
cp $M2/org/apache/hudi/hudi-hive-sync/1.0.2/hudi-hive-sync-1.0.2.jar   $PLUGIN_DIR/
cp $M2/org/apache/hudi/hudi-sync-common/1.0.2/hudi-sync-common-1.0.2.jar $PLUGIN_DIR/

# Copy additional runtime dependencies not in stock Trino 480
cp $M2/com/esotericsoftware/kryo/4.0.2/kryo-4.0.2.jar                   $PLUGIN_DIR/
cp $M2/com/esotericsoftware/minlog/1.3.0/minlog-1.3.0.jar               $PLUGIN_DIR/
cp $M2/org/objenesis/objenesis/3.3/objenesis-3.3.jar                    $PLUGIN_DIR/
cp $M2/org/openjdk/jol/jol-core/0.17/jol-core-0.17.jar                  $PLUGIN_DIR/
```

### Option 2: Docker

```dockerfile
FROM trinodb/trino:480

USER root
RUN rm -f /usr/lib/trino/plugin/hudi/io.trino_trino-hudi-480.jar \
    && rm -f /usr/lib/trino/plugin/hudi/org.apache.hudi_hudi-common-1.1.1.jar \
    && rm -f /usr/lib/trino/plugin/hudi/org.apache.hudi_hudi-io-1.1.1.jar

COPY jars/ /tmp/custom-jars/
RUN cp /tmp/custom-jars/*.jar /usr/lib/trino/plugin/hudi/ \
    && rm -rf /tmp/custom-jars \
    && chown -R trino:trino /usr/lib/trino/plugin/hudi/

USER trino
```

## Catalog Configuration

Create a catalog properties file at `$TRINO_HOME/etc/catalog/hudi.properties`:

### With Hive Metastore (recommended)

```properties
connector.name=hudi
hive.metastore.uri=thrift://<metastore-host>:9083
```

### With AWS Glue Catalog

```properties
connector.name=hudi
hive.metastore=glue
hive.metastore.glue.region=us-east-1
```

### With local filesystem (testing only)

```properties
connector.name=hudi
hive.metastore=file
hive.metastore.catalog.dir=local:///path/to/warehouse
fs.native-local.enabled=true
```

## Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `hudi.metadata-enabled` | `true` | Use Hudi metadata table for file listing |
| `hudi.table-statistics-enabled` | `true` | Enable table statistics for query planning |
| `hudi.target-split-size` | `128MB` | Target split size for parallelism |
| `hudi.split-loader-parallelism` | `10` | Threads for background split loading |
| `hudi.split-generator-parallelism` | `4` | Threads for split generation from partitions |
| `hudi.query-partition-filter-required` | `false` | Require partition filter in queries |
| `hudi.columns-to-hide` | `[]` | Hudi metadata columns to hide |
| `hudi.resolve-column-name-casing-enabled` | `true` | Case-insensitive column name resolution |

## Query Examples

```sql
-- List schemas and tables
SHOW SCHEMAS FROM hudi;
SHOW TABLES FROM hudi.my_database;

-- COW table query
SELECT * FROM hudi.my_database.my_cow_table
WHERE partition_col = 'value';

-- MOR real-time query (base files + log files merged)
SELECT * FROM hudi.my_database.my_mor_table_rt;

-- MOR read-optimized query (base files only, faster but stale)
SELECT * FROM hudi.my_database.my_mor_table_ro;

-- Aggregation with partition pushdown
SELECT region, count(*), avg(amount)
FROM hudi.my_database.orders_rt
WHERE region = 'us_west'
GROUP BY region;
```

## Supported Table Types

| Table Type | Query Mode | Description |
|------------|-----------|-------------|
| Copy-On-Write (COW) | Snapshot | Reads latest compacted parquet files |
| Merge-On-Read (MOR) | Real-time (`_rt`) | Merges base parquet files with delta log files |
| Merge-On-Read (MOR) | Read-optimized (`_ro`) | Reads only base parquet files (no log merge) |
