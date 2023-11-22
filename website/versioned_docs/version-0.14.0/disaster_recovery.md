---
title: Disaster Recovery
toc: true
---

Disaster Recovery is very much mission critical for any software. Especially when it comes to data systems, the impact could be very serious
leading to delay in business decisions or even wrong business decisions at times. Apache Hudi has two operations to assist you in recovering
data from a previous state: "savepoint" and "restore".

## Savepoint

As the name suggest, "savepoint" saves the table as of the commit time, so that it lets you restore the table to this 
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

Savepoint and restore can only be triggered from hudi-cli. Lets walk through an example of how one can take savepoint 
and later restore the state of the table. 

Lets create a hudi table via spark-shell. I am going to trigger few batches of inserts. 

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

// spark-shell
val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)
```

Each batch inserst 10 records. Repeating for 4 more batches. 
```scala

val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)
```

Total record count should be 50. 
```scala
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath)
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select count(partitionpath, uuid) from  hudi_trips_snapshot ").show()

+--------------------------+
|count(partitionpath, uuid)|
  +--------------------------+
|                        50|
  +--------------------------+
```
Let's take a look at the timeline after 5 batch of inserts. 
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

Let's trigger a savepoint as of the latest commit. Savepoint can only be done via hudi-cli.

```sh
./hudi-cli.sh

connect --path /tmp/hudi_trips_cow/
commits show
set --conf SPARK_HOME=<SPARK_HOME>
savepoint create --commit 20220128160245447 --sparkMaster local[2]
```

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

Now, lets continue adding few more batches of inserts. 
Repeat below commands for 3 times.
```scala
val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)
```

Total record count will be 80 since we have done 8 batches in total. (5 until savepoint and 3 after savepoint)
```scala
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath)
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select count(partitionpath, uuid) from  hudi_trips_snapshot ").show()
+--------------------------+
|count(partitionpath, uuid)|
  +--------------------------+
|                        80|
  +--------------------------+
```

Let's say something bad happened and you want to restore your table to a older snapshot. As we called out earlier, we can
trigger restore only from hudi-cli. And do remember to bring down all of your writer processes while doing a restore. 

Lets checkout timeline once, before we trigger the restore.
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

If you are continuing in the same hudi-cli session, you can just execute "refresh" so that table state gets refreshed to 
its latest state. If not, connect to the table again. 

```shell
./hudi-cli.sh

connect --path /tmp/hudi_trips_cow/
commits show
set --conf SPARK_HOME=<SPARK_HOME>
savepoints show
╔═══════════════════╗
║ SavepointTime     ║
╠═══════════════════╣
║ 20220128160245447 ║
╚═══════════════════╝
savepoint rollback --savepoint 20220128160245447 --sparkMaster local[2]
```

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

Lets check the total record count in the table. Should match the records we had, just before we triggered the savepoint. 
```scala
val tripsSnapshotDF = spark.
  read.
  format("hudi").
  load(basePath)
tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

spark.sql("select count(partitionpath, uuid) from  hudi_trips_snapshot ").show()
+--------------------------+
|count(partitionpath, uuid)|
  +--------------------------+
|                        50|
  +--------------------------+
```

As you could see, entire table state is restored back to the commit which was savepointed. Users can choose to trigger savepoint 
at regular cadence and keep deleting older savepoints when new ones are created. Hudi-cli has a command "savepoint delete" 
to assist in deleting a savepoint. Please do remember that cleaner may not clean the files that are savepointed. And so users 
should ensure they delete the savepoints from time to time. If not, the storage reclamation may not happen. 

Note: Savepoint and restore for MOR table is available only from 0.11. 











