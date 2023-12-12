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
# Testing dbt project: `hudi_examples_dbt`

This dbt project transforms demonstrates hudi integration with dbt, it has a few models to demonstrate the different ways in which you can create hudi datasets using dbt.

This directory serves as a self-contained playground dbt project, useful for testing out scripts, and communicating some of the core dbt concepts.

## Setup

Switch working directory and have `python3` installed.

```shell
cd hudi-examples/hudi-examples-dbt
```

## Install dbt

Create python virtual environment ([Reference](https://docs.getdbt.com/docs/installation)).

```shell
python3 -m venv dbt-env
source dbt-env/bin/activate
```

We are using `thrift` as the connection method ([Reference](https://docs.getdbt.com/docs/core/connect-data-platform/spark-setup)).

```shell
python3 -m pip install "dbt-spark[PyHive]"
```

### Configure dbt for Spark

Set up a profile called `spark` to connect to a spark cluster via thrift server ([Reference](https://docs.getdbt.com/docs/core/connect-data-platform/spark-setup#thrift)).

```yaml
spark:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      schema: hudi_examples_dbt
      host: localhost
      port: 10000
      server_side_parameters:
        "spark.driver.memory": "3g"
```

_If you have access to a data warehouse, you can use those credentials – we recommend setting your [target schema](https://docs.getdbt.com/docs/configure-your-profile#section-populating-your-profile) to be a new schema (dbt will create the schema for you, as long as you have the right privileges). If you don't have access to an existing data warehouse, you can also setup a local postgres database and connect to it in your profile._

## Start Spark Thrift server

> **NOTE** Using these versions
> - Spark 3.2.3 (with Derby 10.14.2.0)
> - Hudi 0.14.0

Start a local Derby server

```shell
export DERBY_VERSION=10.14.2.0
wget https://archive.apache.org/dist/db/derby/db-derby-$DERBY_VERSION/db-derby-$DERBY_VERSION-bin.tar.gz -P /opt/
tar -xf /opt/db-derby-$DERBY_VERSION-bin.tar.gz -C /opt/
export DERBY_HOME=/opt/db-derby-$DERBY_VERSION-bin
$DERBY_HOME/bin/startNetworkServer -h 0.0.0.0
```

Start a local Thrift server for Spark

```shell
export SPARK_VERSION=3.2.3
wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz -P /opt/
tar -xf /opt/spark-$SPARK_VERSION-bin-hadoop2.7.tgz -C /opt/
export SPARK_HOME=/opt/spark-$SPARK_VERSION-bin-hadoop2.7

# install dependencies
cp $DERBY_HOME/lib/{derby,derbyclient}.jar $SPARK_HOME/jars/
wget https://repository.apache.org/content/repositories/releases/org/apache/hudi/hudi-spark3.2-bundle_2.12/0.14.0/hudi-spark3.2-bundle_2.12-0.14.0.jar -P $SPARK_HOME/jars/

# start Thrift server connecting to Derby as HMS backend
$SPARK_HOME/sbin/start-thriftserver.sh \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
--conf spark.sql.warehouse.dir=/tmp/hudi/hive/warehouse \
--hiveconf hive.metastore.warehouse.dir=/tmp/hudi/hive/warehouse \
--hiveconf hive.metastore.schema.verification=false \
--hiveconf datanucleus.schema.autoCreateAll=true \
--hiveconf javax.jdo.option.ConnectionDriverName=org.apache.derby.jdbc.ClientDriver \
--hiveconf 'javax.jdo.option.ConnectionURL=jdbc:derby://localhost:1527/default;create=true'
```

## Verify dbt setup

```shell
dbt debug
```

Output of the above command should show this text at the end of the output:

```
All checks passed!
```

## Run the models

### Run `example`

```shell
dbt run -m example
```

<details>
<summary>Output should look like this</summary>

```
05:47:28  Running with dbt=1.0.0
05:47:28  Found 5 models, 10 tests, 0 snapshots, 0 analyses, 0 macros, 0 operations, 0 seed files, 0 sources, 0 exposures, 0 metrics
05:47:28
05:47:29  Concurrency: 1 threads (target='local')
05:47:29
05:47:29  1 of 5 START incremental model analytics.hudi_insert_table...................... [RUN]
05:47:31  1 of 5 OK created incremental model analytics.hudi_insert_table................. [OK in 2.61s]
05:47:31  2 of 5 START incremental model analytics.hudi_insert_overwrite_table............ [RUN]
05:47:34  2 of 5 OK created incremental model analytics.hudi_insert_overwrite_table....... [OK in 3.19s]
05:47:34  3 of 5 START incremental model analytics.hudi_upsert_table...................... [RUN]
05:47:37  3 of 5 OK created incremental model analytics.hudi_upsert_table................. [OK in 2.68s]
05:47:37  4 of 5 START incremental model analytics.hudi_upsert_partitioned_cow_table...... [RUN]
05:47:40  4 of 5 OK created incremental model analytics.hudi_upsert_partitioned_cow_table. [OK in 2.60s]
05:47:40  5 of 5 START incremental model analytics.hudi_upsert_partitioned_mor_table...... [RUN]
05:47:42  5 of 5 OK created incremental model analytics.hudi_upsert_partitioned_mor_table. [OK in 2.53s]
05:47:42
05:47:42  Finished running 5 incremental models in 14.70s.
05:47:42
05:47:42  Completed successfully
```
</details>

### Test `example`

```shell
dbt test -m example
```

<details>
<summary>Output should look like this</summary>

```
05:48:17  Running with dbt=1.0.0
05:48:17  Found 5 models, 10 tests, 0 snapshots, 0 analyses, 0 macros, 0 operations, 0 seed files, 0 sources, 0 exposures, 0 metrics
05:48:17
05:48:19  Concurrency: 1 threads (target='local')
05:48:19
05:48:19  1 of 10 START test not_null_hudi_insert_overwrite_table_id...................... [RUN]
05:48:19  1 of 10 PASS not_null_hudi_insert_overwrite_table_id............................ [PASS in 0.50s]
05:48:19  2 of 10 START test not_null_hudi_insert_overwrite_table_name.................... [RUN]
05:48:20  2 of 10 PASS not_null_hudi_insert_overwrite_table_name.......................... [PASS in 0.45s]
05:48:20  3 of 10 START test not_null_hudi_insert_overwrite_table_ts...................... [RUN]
05:48:20  3 of 10 PASS not_null_hudi_insert_overwrite_table_ts............................ [PASS in 0.47s]
05:48:20  4 of 10 START test not_null_hudi_insert_table_id................................ [RUN]
05:48:20  4 of 10 PASS not_null_hudi_insert_table_id...................................... [PASS in 0.44s]
05:48:20  5 of 10 START test not_null_hudi_upsert_table_id................................ [RUN]
05:48:21  5 of 10 PASS not_null_hudi_upsert_table_id...................................... [PASS in 0.38s]
05:48:21  6 of 10 START test not_null_hudi_upsert_table_name.............................. [RUN]
05:48:21  6 of 10 PASS not_null_hudi_upsert_table_name.................................... [PASS in 0.40s]
05:48:21  7 of 10 START test not_null_hudi_upsert_table_ts................................ [RUN]
05:48:22  7 of 10 PASS not_null_hudi_upsert_table_ts...................................... [PASS in 0.38s]
05:48:22  8 of 10 START test unique_hudi_insert_overwrite_table_id........................ [RUN]
05:48:23  8 of 10 PASS unique_hudi_insert_overwrite_table_id.............................. [PASS in 1.32s]
05:48:23  9 of 10 START test unique_hudi_insert_table_id.................................. [RUN]
05:48:24  9 of 10 PASS unique_hudi_insert_table_id........................................ [PASS in 1.26s]
05:48:24  10 of 10 START test unique_hudi_upsert_table_id................................. [RUN]
05:48:25  10 of 10 PASS unique_hudi_upsert_table_id....................................... [PASS in 1.29s]
05:48:26
05:48:26  Finished running 10 tests in 8.23s.
05:48:26
05:48:26  Completed successfully
05:48:26
05:48:26  Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
```
</details>

### Run `example_cdc`

Bootstrap the raw table `raw_updates` and `profiles`.

```shell
dbt run -m example_cdc.raw_updates -m example_cdc.profiles
```

Launch a `spark-sql` shell to interact with the tables created by `example_cdc`.

```shell
spark-sql \
--packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.14.0 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
--conf spark.sql.warehouse.dir=/tmp/hudi/hive/warehouse \
--conf spark.hadoop.hive.metastore.warehouse.dir=/tmp/hudi/hive/warehouse \
--conf spark.hadoop.hive.metastore.schema.verification=false \
--conf spark.hadoop.datanucleus.schema.autoCreateAll=true \
--conf spark.hadoop.javax.jdo.option.ConnectionDriverName=org.apache.derby.jdbc.ClientDriver \
--conf 'spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby://localhost:1527/default;create=true' \
--conf 'spark.hadoop.hive.cli.print.header=true'
```

Insert sample records.

```sql
use hudi_examples_dbt;
insert into raw_updates values ('101', 'D', UNIX_TIMESTAMP());
insert into raw_updates values ('102', 'E', UNIX_TIMESTAMP());
insert into raw_updates values ('103', 'F', UNIX_TIMESTAMP());
```

Process the updates and write new date to `profiles`.

```shell
dbt run -m example_cdc.profiles
```

<details>
<summary>Check `profiles` records.</summary>

```shell
spark-sql> refresh table profiles;
spark-sql> select _hoodie_commit_time, user_id, city, updated_at from profiles order by updated_at;
_hoodie_commit_time	user_id	city	updated_at
20231128013722030	101	D	1701157027
20231128013722030	102	E	1701157031
20231128013722030	103	F	1701157035
Time taken: 0.219 seconds, Fetched 3 row(s)
```
</details>

Extract changed data from `profiles` to `profile_changes`.

```shell
dbt run -m example_cdc.profile_changes
```

<details>
<summary>Check `profile_changes` records.</summary>

```shell
spark-sql> refresh table profile_changes;
spark-sql> select user_id, old_city, new_city from profile_changes order by process_ts;
user_id	old_city	new_city
101	Nil	A
102	Nil	B
103	Nil	C
101	A	D
102	B	E
103	C	F
Time taken: 0.129 seconds, Fetched 6 row(s)
```
</details>

### Generate documentation

```shell
dbt docs generate
dbt docs serve
# then visit http://127.0.0.1:8080/#!/overview
```

---
For more information on dbt:
- Read the [introduction to dbt](https://docs.getdbt.com/docs/introduction).
- Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint).
- Join the [dbt community](http://community.getdbt.com/).
---
