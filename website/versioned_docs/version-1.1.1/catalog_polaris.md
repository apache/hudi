---
title: Apache Polaris (Incubating)
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
keywords: [hudi, polaris, catalog, integration]
---

:::warning Polaris Integration Status
Hudi 1.1.0 added support for Apache Polaris catalog integration (see [PR #13558](https://github.com/apache/hudi/pull/13558)). However, a Polaris release that includes [this PR](https://github.com/apache/polaris/pull/1862) is pending before this integration to be available.
:::

## Overview

Apache Hudi integrates with [Apache Polaris](https://polaris.apache.org/) (Incubating) catalog by delegating table creation operations to the Polaris Spark client. This integration allows Hudi tables to be automatically registered in the Polaris Catalog when created through Spark SQL, enabling unified metadata management across your data lakehouse.

The integration works by detecting when Polaris catalog is configured in your Spark session and delegating the `createTable` operation to the Polaris Spark catalog implementation, ensuring that Hudi table metadata is properly registered in Polaris.

## How It Works

When Polaris catalog is configured, Hudi automatically detects it and routes table creation operations to Polaris. This means:

- Hudi tables created via Spark SQL are automatically registered in the Polaris catalog
- Tables remain fully functional Hudi tables with all Hudi features (time travel, incremental queries, etc.)
- Tables are discoverable and queryable through Polaris catalog interfaces

## Configuration

To enable Polaris catalog integration, you need to configure both the Polaris catalog in Spark and specify the catalog class name in Hudi configuration.

### Spark Catalog Configuration

First, configure the Polaris catalog in your Spark session:

```sql
set spark.sql.catalog.polaris_catalog=org.apache.polaris.spark.SparkCatalog
```

### Hudi Configuration

Configure Hudi to use the Polaris catalog class:

```properties
hoodie.spark.polaris.catalog.class=org.apache.polaris.spark.SparkCatalog
```

**Configuration Property:**

| Property                             | Default                                 | Description                                                                        |
|--------------------------------------|-----------------------------------------|------------------------------------------------------------------------------------|
| `hoodie.spark.polaris.catalog.class` | `org.apache.polaris.spark.SparkCatalog` | Fully qualified class name of the catalog that is used by the Polaris Spark client |

This configuration property was introduced in Hudi 1.1.0 and is marked as advanced. The default value matches the standard Polaris Spark catalog implementation.

## Usage Examples

### Creating a Hudi Table with Polaris Catalog

Once Polaris catalog is configured, you can create Hudi tables using Spark SQL. Hudi will automatically detect the Polaris catalog configuration and delegate table registration to Polaris:

```sql
CREATE TABLE IF NOT EXISTS mydb.hudi_table (
  id INT,
  name STRING,
  ts BIGINT,
  dt STRING
) USING hudi
PARTITIONED BY (dt)
TBLPROPERTIES (
  primaryKey = 'id'
)
```

### Writing Data to Hudi Tables

You can write data to Hudi tables using `INSERT INTO` SQL statements:

```sql
INSERT INTO mydb.hudi_table
SELECT 1 AS id, 'John' AS name, 1695159649087 AS ts, '2024-01-01' AS dt;

-- Insert with dynamic partitioning
INSERT INTO mydb.hudi_table PARTITION(dt)
SELECT 2 AS id, 'Jane' AS name, 1695159649088 AS ts, '2024-01-02' AS dt;
```

### Querying Hudi Tables

Query Hudi tables normally - they will be accessible through the configured catalog:

```sql
SELECT * FROM mydb.hudi_table
WHERE dt = '2024-01-01'
```
