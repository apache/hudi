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

# Hudi Upgrade/Downgrade Test Fixtures

This directory contains pre-created MOR Hudi tables from different releases used for testing upgrade/downgrade functionality.

## Fixture Tables

| Directory | Hudi Version | Table Version |
|-----------|--------------|---------------|
| `hudi-v4-table/` | 0.11.1       | 4 |
| `hudi-v5-table/` | 0.12.2       | 5 |
| `hudi-v6-table/` | 0.14.0       | 6 |
| `hudi-v8-table/` | 1.0.2        | 8 |
| `hudi-v9-table/` | 1.1.0        | 9 |

## Table Schema

All fixture tables use a consistent simple schema:
- `id` (string) - Record identifier
- `name` (string) - Record name  
- `ts` (long) - Timestamp
- `partition` (string) - Partition value

## Table Structure

Each fixture table contains:
- 2-3 base files (parquet)
- 2-3 log files 
- Multiple committed instants
- 1 pending/failed write (for rollback testing)
- Basic .hoodie metadata structure

## Generating Fixtures

### Prerequisites
- Java 8+ installed
- Internet connection (for downloading Spark binaries and Hudi bundles via Maven)

### Generation Process

Use the `generate-fixtures.sh` script to create all fixture tables:

```bash
./generate-fixtures.sh
```

**Note**: The script will create fixture tables in the `mor-tables/` directory. On first run, it downloads and caches Spark binaries in the `spark-versions/` directory. Each fixture generation may take several minutes as it downloads Spark binaries and Hudi bundles, then creates table data.

### Script Parameters

The `generate-fixtures.sh` script supports the following parameters:

| Parameter | Description | Required | Example |
|-----------|-------------|----------|---------|
| `--version <version_list>` | Comma-separated list of table versions to generate | No | `--version 4,5,6` |
| `--hudi-bundle-path <path>` | Path to locally built Hudi bundle JAR (required for version 9) | Only for version 9 | `--hudi-bundle-path /path/to/bundle.jar` |

#### Supported Versions
- **4** - Hudi 0.11.1 (Spark 3.2.4, Scala 2.12)
- **5** - Hudi 0.12.2 (Spark 3.3.1, Scala 2.12)  
- **6** - Hudi 0.14.0 (Spark 3.4.1, Scala 2.12)
- **8** - Hudi 1.0.2 (Spark 3.5.4, Scala 2.12)
- **9** - Hudi 1.1.0 (Spark 3.5.4, Scala 2.12) - **Requires local bundle**

#### Usage Examples

```bash
# Generate all available versions (4,5,6,8) - version 9 excluded due to local bundle requirement
./generate-fixtures.sh

# Generate specific versions only
./generate-fixtures.sh --version 4,5

# Generate only version 6
./generate-fixtures.sh --version 6

# Generate version 9 (requires locally built Hudi bundle)
./generate-fixtures.sh --version 9 --hudi-bundle-path /path/to/hudi-spark3.5-bundle_2.12-1.1.0-SNAPSHOT.jar

# Generate multiple versions including version 9
./generate-fixtures.sh --version 4,6,9 --hudi-bundle-path /path/to/bundle.jar
```

#### Version 9 Special Requirements

Version 9 requires a locally built Hudi bundle since Hudi 1.1.0 is not yet officially released. To build the bundle:

```bash
# In your Hudi repository
cd <hudi-repo>
mvn clean install -DskipTests -Dspark3.5 -Dscala-2.12 -pl packaging/hudi-spark-bundle -am

# Then use the generated bundle
./generate-fixtures.sh --version 9 --hudi-bundle-path <hudi-repo>/packaging/hudi-spark-bundle/target/hudi-spark3.5-bundle_2.12-1.1.0-SNAPSHOT.jar
```

**Note**: If you try to generate version 9 without providing `--hudi-bundle-path`, the script will display detailed build instructions and exit with an error.

### Spark Binaries and Compatibility Matrix

The script downloads and caches official Apache Spark binaries with Hudi bundles resolved via the `--packages` flag:

| Hudi Version | Table Version | Spark Version | Scala Version | Downloaded Binary |
|--------------|---------------|---------------|---------------|-------------------|
| 0.11.1       | 4             | 3.2.4         | 2.12          | spark-3.2.4-bin-hadoop3.2.tgz |
| 0.12.2       | 5             | 3.3.1         | 2.12          | spark-3.3.1-bin-hadoop3.tgz |
| 0.14.0       | 6             | 3.4.1         | 2.12          | spark-3.4.1-bin-hadoop3.tgz |
| 1.0.2        | 8             | 3.5.4         | 2.12          | spark-3.5.4-bin-hadoop3.tgz |
| 1.1.0        | 9             | 3.5.4         | 2.12          | spark-3.5.4-bin-hadoop3.tgz |

### Manual Generation Example

The script uses a template-based approach with separate Scala files and variable substitution. Here's how to manually replicate the process:

#### Hudi 0.11.1 (Version 4)
```bash
# 1. Download and extract Spark 3.2.4 binary (if not already present)
mkdir -p spark-versions
cd spark-versions
wget https://archive.apache.org/dist/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz
tar -xzf spark-3.2.4-bin-hadoop3.2.tgz
cd ..

# 2. Create custom Scala script from template
cp scala-templates/generate-fixture.scala /tmp/generate_hudi-v4-table.scala

# 3. Substitute template variables in the copied script
sed -i.bak \
    -e 's/${TABLE_NAME}/hudi-v4-table_table/g' \
    -e 's|${BASE_PATH}|'$(pwd)'/mor-tables/hudi-v4-table|g' \
    /tmp/generate_hudi-v4-table.scala

# 4. Run spark-shell with the customized Scala script using -i flag
./spark-versions/spark-3.2.4-bin-hadoop3.2/bin/spark-shell \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.jars.ivy=/tmp/ivy-cache-hudi-v4-table' \
  --conf 'spark.sql.warehouse.dir=/tmp/spark-warehouse' \
  --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.1 \
  -i /tmp/generate_hudi-v4-table.scala

# 5. Clean up temporary files
rm -f /tmp/generate_hudi-v4-table.scala /tmp/generate_hudi-v4-table.scala.bak
rm -rf /tmp/ivy-cache-hudi-v4-table
```

**Note**: The Scala code itself is in `scala-templates/generate-fixture.scala` and contains template variables like `${TABLE_NAME}` and `${BASE_PATH}` that get replaced by the shell script.

#### Other Versions
For other versions, use the same template-based pattern but with the appropriate Spark binary and Hudi bundle version from the compatibility matrix above. The key differences are:

- **Hudi 0.12.2 (Version 5)**:
  - Spark binary: `./spark-versions/spark-3.3.1-bin-hadoop3/bin/spark-shell`
  - Hudi bundle: `--packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2`
  - Table name: `hudi-v5-table_table`, Base path: `mor-tables/hudi-v5-table`

- **Hudi 0.14.0 (Version 6)**:
  - Spark binary: `./spark-versions/spark-3.4.1-bin-hadoop3/bin/spark-shell`
  - Hudi bundle: `--packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0`
  - Table name: `hudi-v6-table_table`, Base path: `mor-tables/hudi-v6-table`

- **Hudi 1.0.2 (Version 8)**:
  - Spark binary: `./spark-versions/spark-3.5.4-bin-hadoop3/bin/spark-shell`
  - Hudi bundle: `--packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2`
  - Table name: `hudi-v8-table_table`, Base path: `mor-tables/hudi-v8-table`

- **Hudi 1.1.0 (Version 9)**: Requires `--jars <local-bundle-path>` instead of `--packages` (see version 9 requirements above)


## Notes

- Fixtures are copied to temporary directories during testing to avoid modifications
- Each fixture should be self-contained with all necessary metadata
- Keep fixtures minimal but realistic (small data sizes for fast tests)
- Ensure consistent schema across all versions for compatibility testing
