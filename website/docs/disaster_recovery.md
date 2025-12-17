---
title: Disaster Recovery
toc: true
---

Disaster Recovery is very much mission-critical for any software. Especially when it comes to data systems, the impact could be very serious
leading to delay in business decisions or even wrong business decisions at times. Apache Hudi has two operations to assist you in recovering
data from a previous state: `savepoint` and `restore`.

## Savepoint

As the name suggest, `savepoint` saves the table as of the commit time, so that it lets you restore the table to this
savepoint at a later point in time if need be. Care is taken to ensure cleaner will not clean up any files that are savepointed.
On similar lines, savepoint cannot be triggered on a commit that is already cleaned up. In simpler terms, this is synonymous
to taking a backup, just that we don't make a new copy of the table, but just save the state of the table elegantly so that
we can restore it later when in need.

## Restore

This operation lets you restore your table to one of the savepoint commit. This operation cannot be undone (or reversed) and so care
should be taken before doing a restore. Hudi will delete all data files and commit files (timeline files) greater than the
savepoint commit to which the table is being restored. You should pause all writes to the table when performing
a restore since they are likely to fail while the restore is in progress. Also, reads could also fail since snapshot queries
will be hitting latest files which has high possibility of getting deleted with restore.

## Runbook

Savepoint and restore can be triggered via [Hudi CLI](cli.md) and [SQL Procedures](procedures.md). Let's walk through an example of how 
one can take savepoint and later restore the state of the table.

**Note:** When using the Hudi CLI, we need to specify the *table path*, whereas when using SQL procedures, we need to provide the *table name*.

Let's create a hudi table via `spark-shell` and trigger a batch of inserts.

```scala
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val tableName = "hudi_trips_cow"
val basePath = "file:///tmp/hudi_trips_cow"
val dataGen = new DataGenerator

val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option("hoodie.table.ordering.fields", "ts").
  option("hoodie.datasource.write.recordkey.field", "uuid").
  option("hoodie.datasource.write.partitionpath.field", "partitionpath").
  option("hoodie.table.name", tableName).
  mode(Overwrite).
  save(basePath)
```

Let's add four more batches of inserts.
```scala
for (_ <- 1 to 4) {
  val inserts = convertToStringList(dataGen.generateInserts(10))
  val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
  df.write.format("hudi").
    options(getQuickstartWriteConfigs).
    option("hoodie.table.ordering.fields", "ts").
    option("hoodie.datasource.write.recordkey.field", "uuid").
    option("hoodie.datasource.write.partitionpath.field", "partitionpath").
    option("hoodie.table.name", tableName).
    mode(Append).
    save(basePath)
}
```

Total record count should be 50.
```scala
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath)
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select count(partitionpath, uuid) from  hudi_trips_snapshot").show()

+--------------------------+
|count(partitionpath, uuid)|
+--------------------------+
|                        50|
+--------------------------+
```
Let's take a look at the timeline after 5 batches of inserts.
```shell
ls -ltr /tmp/hudi_trips_cow/.hoodie 
total 128
drwxr-xr-x  2 nsb  wheel    64 Jan 28 16:00 archived
-rw-r--r--  1 nsb  wheel   546 Jan 28 16:00 hoodie.properties
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:00 20220128160040171.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:00 20220128160040171.inflight
-rw-r--r--  1 nsb  wheel  4374 Jan 28 16:00 20220128160040171.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:01 20220128160124637.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:01 20220128160124637.inflight
-rw-r--r--  1 nsb  wheel  4414 Jan 28 16:01 20220128160124637.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160226172.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160226172.inflight
-rw-r--r--  1 nsb  wheel  4427 Jan 28 16:02 20220128160226172.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160229636.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160229636.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:02 20220128160229636.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160245447.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160245447.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:02 20220128160245447.commit
```

### Savepoint Example

Savepoints can be created via [Hudi CLI](cli.md) or [SQL Procedures](procedures.md).

Let's trigger a savepoint as of the latest commit.

#### Using Hudi CLI

1. Launch the Hudi CLI.
2. Specify the SPARK_HOME if it's not specified.

```sh
cd hudi-cli
./hudi-cli.sh
set --conf SPARK_HOME=<SPARK_HOME>
```

3. Connect to the table using the table path for example `/tmp/hudi_trips_cow/`.
4. Run the `commits show` command to display the commits from the table.
5. Run the `savepoint create` command by specifying the `commit_time` to create the Savepoint.

```sh
connect --path /tmp/hudi_trips_cow/
commits show
savepoint create --commit 20220128160245447 --sparkMaster local[2]
```

:::note NOTE:
Make sure you replace 20220128160245447 with the latest commit in your table.
:::

#### Using Spark SQL Procedures

1. Launch the `spark-sql` shell by specifying Spark version and Hudi version. For example,

```shell
export SPARK_VERSION=3.5
export HUDI_VERSION=1.0.2

spark-sql --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:$HUDI_VERSION \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```

2. Run the `show_commits` command to display the commits from the table.
3. Run the `create_savepoint` command by specifying the commit_time to create the Savepoint.

```sql
call show_commits(table => 'hudi_trips_cow');
call create_savepoint(table => 'hudi_trips_cow', commit_time => '20220128160245447');
```

:::note NOTE:
Make sure you replace 20220128160245447 with the latest commit in your table.
:::

Let's check the timeline after savepoint.
```shell
ls -ltr /tmp/hudi_trips_cow/.hoodie
total 136
drwxr-xr-x  2 nsb  wheel    64 Jan 28 16:00 archived
-rw-r--r--  1 nsb  wheel   546 Jan 28 16:00 hoodie.properties
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:00 20220128160040171.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:00 20220128160040171.inflight
-rw-r--r--  1 nsb  wheel  4374 Jan 28 16:00 20220128160040171.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:01 20220128160124637.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:01 20220128160124637.inflight
-rw-r--r--  1 nsb  wheel  4414 Jan 28 16:01 20220128160124637.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160226172.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160226172.inflight
-rw-r--r--  1 nsb  wheel  4427 Jan 28 16:02 20220128160226172.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160229636.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160229636.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:02 20220128160229636.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160245447.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160245447.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:02 20220128160245447.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:05 20220128160245447.savepoint.inflight
-rw-r--r--  1 nsb  wheel  1168 Jan 28 16:05 20220128160245447.savepoint
```

You could notice that savepoint meta files are added which keeps track of the files that are part of the latest table snapshot.

Now, let's continue adding three more batches of inserts.

```scala
for (_ <- 1 to 3) {
  val inserts = convertToStringList(dataGen.generateInserts(10))
  val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
  df.write.format("hudi").
    options(getQuickstartWriteConfigs).
    option("hoodie.table.ordering.fields", "ts").
    option("hoodie.datasource.write.recordkey.field", "uuid").
    option("hoodie.datasource.write.partitionpath.field", "partitionpath").
    option("hoodie.table.name", tableName).
    mode(Append).
    save(basePath)
}
```

Total record count will be 80 since we have done 8 batches in total. (5 until savepoint and 3 after savepoint)
```scala
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath)
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select count(partitionpath, uuid) from  hudi_trips_snapshot").show()
+--------------------------+
|count(partitionpath, uuid)|
+--------------------------+
|                        80|
+--------------------------+
```

### Restore Example

Let's say something bad happened, and you want to restore your table to an older snapshot. We can perform a restore operation via [Hudi CLI](cli.md) or [SQL Procedures](procedures.md). And do remember to bring down all of your writer processes while doing a restore.

Let's checkout timeline once, before we trigger the restore.
```shell
ls -ltr /tmp/hudi_trips_cow/.hoodie
total 208
drwxr-xr-x  2 nsb  wheel    64 Jan 28 16:00 archived
-rw-r--r--  1 nsb  wheel   546 Jan 28 16:00 hoodie.properties
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:00 20220128160040171.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:00 20220128160040171.inflight
-rw-r--r--  1 nsb  wheel  4374 Jan 28 16:00 20220128160040171.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:01 20220128160124637.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:01 20220128160124637.inflight
-rw-r--r--  1 nsb  wheel  4414 Jan 28 16:01 20220128160124637.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160226172.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160226172.inflight
-rw-r--r--  1 nsb  wheel  4427 Jan 28 16:02 20220128160226172.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160229636.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160229636.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:02 20220128160229636.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160245447.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160245447.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:02 20220128160245447.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:05 20220128160245447.savepoint.inflight
-rw-r--r--  1 nsb  wheel  1168 Jan 28 16:05 20220128160245447.savepoint
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:06 20220128160620557.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:06 20220128160620557.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:06 20220128160620557.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:06 20220128160627501.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:06 20220128160627501.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:06 20220128160627501.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:06 20220128160630785.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:06 20220128160630785.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:06 20220128160630785.commit
```

#### Using Hudi CLI

1. Launch the Hudi CLI or use the existing Hudi CLI.
2. Specify the SPARK_HOME if it's not specified.

```shell
cd hudi-cli
./hudi-cli.sh
set --conf SPARK_HOME=<SPARK_HOME>
```

3. Connect to the Hudi table using the specified table path, for example `/tmp/hudi_trips_cow/`.
4. Execute the `refresh` command to update the table state to its latest version.
5. Run the `savepoints show` command to display the all savepoints.
6. Run the `savepoint rollback` specifying the savepoint **instant_time** to perform the rollback.
7. (Optional) Run the `savepoint delete` command to delete the savepoint **instant_time** from the existing savepoints.

```sh
connect --path /tmp/hudi_trips_cow/
refresh
savepoints show
╔═══════════════════╗
║ SavepointTime     ║
╠═══════════════════╣
║ 20220128160245447 ║
╚═══════════════════╝
savepoint rollback --savepoint 20220128160245447 --sparkMaster local[2]
savepoint delete --commit 20220128160245447 --sparkMaster local[2]
```

:::note NOTE:
Make sure you replace 20220128160245447 with the latest savepoint in your table.
:::

#### Using Spark SQL Procedures

1. Launch the `spark-sql` shell by specifying Spark version and Hudi version or use the existing `spark-sql` shell.

```sh
export SPARK_VERSION=3.5
export HUDI_VERSION=1.0.2

spark-sql --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:$HUDI_VERSION \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```

2. Run the `show_savepoints` command to display all the savepoints from the table.
3. Run the `rollback_to_savepoint` command by specifying the savepoint **instant_time** to rollback.
4. (Optional) Run the `delete_savepoint` command to delete the savepoint **instant_time** from the existing savepoints.

```sql
call show_savepoints(table => 'hudi_trips_cow');
call rollback_to_savepoint(table => 'hudi_trips_cow', instant_time => '20220128160245447');
call delete_savepoint(table => 'hudi_trips_cow', instant_time => '20220128160245447');
```

:::note NOTE:
Make sure you replace 20220128160245447 with the latest savepoint in your table.
:::

Hudi table should have been restored to the savepointed commit 20220128160245447. Both data files and timeline files should have
been deleted.
```shell
ls -ltr /tmp/hudi_trips_cow/.hoodie
total 152
drwxr-xr-x  2 nsb  wheel    64 Jan 28 16:00 archived
-rw-r--r--  1 nsb  wheel   546 Jan 28 16:00 hoodie.properties
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:00 20220128160040171.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:00 20220128160040171.inflight
-rw-r--r--  1 nsb  wheel  4374 Jan 28 16:00 20220128160040171.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:01 20220128160124637.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:01 20220128160124637.inflight
-rw-r--r--  1 nsb  wheel  4414 Jan 28 16:01 20220128160124637.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160226172.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160226172.inflight
-rw-r--r--  1 nsb  wheel  4427 Jan 28 16:02 20220128160226172.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160229636.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160229636.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:02 20220128160229636.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:02 20220128160245447.commit.requested
-rw-r--r--  1 nsb  wheel  2594 Jan 28 16:02 20220128160245447.inflight
-rw-r--r--  1 nsb  wheel  4428 Jan 28 16:02 20220128160245447.commit
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:05 20220128160245447.savepoint.inflight
-rw-r--r--  1 nsb  wheel  1168 Jan 28 16:05 20220128160245447.savepoint
-rw-r--r--  1 nsb  wheel     0 Jan 28 16:07 20220128160732437.restore.inflight
-rw-r--r--  1 nsb  wheel  4152 Jan 28 16:07 20220128160732437.restore
```

Let's check the total record count in the table. Should match the records we had, just before we triggered the savepoint.
```scala
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath)
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select count(partitionpath, uuid) from  hudi_trips_snapshot").show()
+--------------------------+
|count(partitionpath, uuid)|
+--------------------------+
|                        50|
+--------------------------+
```

As you could see, entire table state is restored back to the commit which was savepointed. Users can choose to trigger savepoint
at regular cadence and keep deleting older savepoints when new ones are created. Please do remember that cleaner may not clean the files 
that are savepointed. And so users should ensure they delete the savepoints from time to time. If not, the storage reclamation may not happen.

**Note:** Savepoint and restore for **MOR** table is available only from **0.11**. 

## Related Resources
<h3>Videos</h3>

* [Use Glue 4.0 to take regular save points for your Hudi tables for backup or disaster Recovery](https://www.youtube.com/watch?v=VgIMPSK7rFAa)
* [How to Rollback to Previous Checkpoint during Disaster in Apache Hudi using Glue 4.0 Demo](https://www.youtube.com/watch?v=Vi25q4vzogs)








