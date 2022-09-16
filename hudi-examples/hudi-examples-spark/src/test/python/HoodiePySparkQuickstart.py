#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
 
#       http://www.apache.org/licenses/LICENSE-2.0
 
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
 

import sys
import os
from pyspark import sql
import random
from pyspark.sql.functions import lit
from functools import reduce
import tempfile
import argparse


class ExamplePySpark:
    def __init__(self, spark: sql.SparkSession, tableName: str, basePath: str):
        self.spark = spark
        self.tableName = tableName
        self.basePath = basePath + "/" + tableName
        self.hudi_options = {
            'hoodie.table.name': tableName,
            'hoodie.datasource.write.recordkey.field': 'uuid',
            'hoodie.datasource.write.partitionpath.field': 'partitionpath',
            'hoodie.datasource.write.table.name': tableName,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.precombine.field': 'ts',
            'hoodie.upsert.shuffle.parallelism': 2,
            'hoodie.insert.shuffle.parallelism': 2
        }

        self.dataGen = spark._jvm.org.apache.hudi.QuickstartUtils.DataGenerator()
        self.snapshotQuery = "SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM hudi_trips_snapshot"
        return

    def runQuickstart(self):
        
        def snap():
            return self.spark.sql(self.snapshotQuery)
        insertDf = self.insertData()
        self.queryData()
        assert len(insertDf.exceptAll(snap()).collect()) == 0
        
        snapshotBeforeUpdate = snap()
        updateDf = self.updateData()
        self.queryData()
        assert len(snap().intersect(updateDf).collect()) == len(updateDf.collect())
        assert len(snap().exceptAll(updateDf).exceptAll(snapshotBeforeUpdate).collect()) == 0


        self.timeTravelQuery()
        self.incrementalQuery()
        self.pointInTimeQuery()

        self.softDeletes()
        self.queryData()

        snapshotBeforeDelete = snap()
        deletesDf = self.hardDeletes()
        self.queryData()
        assert len(snap().select(["uuid", "partitionpath", "ts"]).intersect(deletesDf).collect()) == 0
        assert len(snapshotBeforeDelete.exceptAll(snap()).exceptAll(snapshotBeforeDelete).collect()) == 0

        snapshotBeforeInsertOverwrite = snap()    
        insertOverwriteDf = self.insertOverwrite()
        self.queryData()
        withoutSanFran = snapshotBeforeInsertOverwrite.filter("partitionpath != 'americas/united_states/san_francisco'")
        expectedDf = withoutSanFran.union(insertOverwriteDf)
        assert len(snap().exceptAll(expectedDf).collect()) == 0
        return

    def insertData(self):
        print("Insert Data")
        inserts = self.spark._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(self.dataGen.generateInserts(10))
        df = self.spark.read.json(self.spark.sparkContext.parallelize(inserts, 2))
        df.write.format("hudi").options(**self.hudi_options).mode("overwrite").save(self.basePath)
        return df

    def updateData(self):
        print("Update Data")
        updates = self.spark._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(self.dataGen.generateUniqueUpdates(5))
        df = self.spark.read.json(spark.sparkContext.parallelize(updates, 2))
        df.write.format("hudi").options(**self.hudi_options).mode("append").save(self.basePath)
        return df

    def queryData(self):
        print("Query Data")
        tripsSnapshotDF = self.spark.read.format("hudi").load(self.basePath)
        tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
        self.spark.sql("SELECT fare, begin_lon, begin_lat, ts FROM  hudi_trips_snapshot WHERE fare > 20.0").show()
        self.spark.sql("SELECT _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare FROM  hudi_trips_snapshot").show()
        return

    def timeTravelQuery(self):
        query = "SELECT begin_lat, begin_lon, driver, end_lat, end_lon, fare, partitionpath, rider, ts, uuid FROM time_travel_query"
        print("Time Travel Query")
        self.spark.read.format("hudi").option("as.of.instant", "20210728141108").load(self.basePath).createOrReplaceTempView("time_travel_query")
        self.spark.sql(query)
        self.spark.read.format("hudi").option("as.of.instant", "2021-07-28 14:11:08.000").load(self.basePath).createOrReplaceTempView("time_travel_query")
        self.spark.sql(query)
        self.spark.read.format("hudi").option("as.of.instant", "2021-07-28").load(self.basePath).createOrReplaceTempView("time_travel_query")
        self.spark.sql(query)
        return
    
    def incrementalQuery(self):
        print("Incremental Query")
        self.spark.read.format("hudi").load(self.basePath).createOrReplaceTempView("hudi_trips_snapshot")
        self.commits = list(map(lambda row: row[0], self.spark.sql("SELECT DISTINCT(_hoodie_commit_time) AS commitTime FROM  hudi_trips_snapshot ORDER BY commitTime").limit(50).collect()))
        beginTime = self.commits[len(self.commits) - 2] 
        incremental_read_options = {
            'hoodie.datasource.query.type': 'incremental',
            'hoodie.datasource.read.begin.instanttime': beginTime,
        }
        tripsIncrementalDF = self.spark.read.format("hudi").options(**incremental_read_options).load(self.basePath)
        tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")
        self.spark.sql("SELECT `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts FROM hudi_trips_incremental WHERE fare > 20.0").show()

    def pointInTimeQuery(self):
        print("Point-in-time Query")
        beginTime = "000"
        endTime = self.commits[len(self.commits) - 2]
        point_in_time_read_options = {
            'hoodie.datasource.query.type': 'incremental',
            'hoodie.datasource.read.end.instanttime': endTime,
            'hoodie.datasource.read.begin.instanttime': beginTime
        }

        tripsPointInTimeDF = self.spark.read.format("hudi").options(**point_in_time_read_options).load(self.basePath)
        tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")
        self.spark.sql("SELECT `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts FROM hudi_trips_point_in_time WHERE fare > 20.0").show()
    
    def softDeletes(self):
        print("Soft Deletes")
        spark.read.format("hudi").load(self.basePath).createOrReplaceTempView("hudi_trips_snapshot")

        # fetch total records count
        trip_count = spark.sql("SELECT uuid, partitionpath FROM hudi_trips_snapshot").count()
        non_null_rider_count = spark.sql("SELECT uuid, partitionpath FROM hudi_trips_snapshot WHERE rider IS NOT null").count()
        print(f"trip count: {trip_count}, non null rider count: {non_null_rider_count}")
        # fetch two records for soft deletes
        soft_delete_ds = spark.sql("SELECT * FROM hudi_trips_snapshot").limit(2)
        # prepare the soft deletes by ensuring the appropriate fields are nullified
        meta_columns = ["_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", 
        "_hoodie_partition_path", "_hoodie_file_name"]
        excluded_columns = meta_columns + ["ts", "uuid", "partitionpath"]
        nullify_columns = list(filter(lambda field: field[0] not in excluded_columns, \
        list(map(lambda field: (field.name, field.dataType), soft_delete_ds.schema.fields))))

        hudi_soft_delete_options = {
        'hoodie.table.name': self.tableName,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'partitionpath',
        'hoodie.datasource.write.table.name': self.tableName,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.upsert.shuffle.parallelism': 2, 
        'hoodie.insert.shuffle.parallelism': 2
        }

        soft_delete_df = reduce(lambda df,col: df.withColumn(col[0], lit(None).cast(col[1])), \
        nullify_columns, reduce(lambda df,col: df.drop(col[0]), meta_columns, soft_delete_ds))

        # simply upsert the table after setting these fields to null
        soft_delete_df.write.format("hudi").options(**hudi_soft_delete_options).mode("append").save(self.basePath)

        # reload data
        self.spark.read.format("hudi").load(self.basePath).createOrReplaceTempView("hudi_trips_snapshot")

        # This should return the same total count as before
        trip_count = self.spark.sql("SELECT uuid, partitionpath FROM hudi_trips_snapshot").count()
        # This should return (total - 2) count as two records are updated with nulls
        non_null_rider_count = self.spark.sql("SELECT uuid, partitionpath FROM hudi_trips_snapshot WHERE rider IS NOT null").count()
        print(f"trip count: {trip_count}, non null rider count: {non_null_rider_count}")

    def hardDeletes(self):
        print("Hard Deletes")
        # fetch total records count
        total_count = self.spark.sql("SELECT uuid, partitionpath FROM hudi_trips_snapshot").count()
        print(f"total count: {total_count}")
        # fetch two records to be deleted
        ds = self.spark.sql("SELECT uuid, partitionpath FROM hudi_trips_snapshot").limit(2)

        # issue deletes
        hudi_hard_delete_options = {
            'hoodie.table.name': self.tableName,
            'hoodie.datasource.write.recordkey.field': 'uuid',
            'hoodie.datasource.write.partitionpath.field': 'partitionpath',
            'hoodie.datasource.write.table.name': self.tableName,
            'hoodie.datasource.write.operation': 'delete',
            'hoodie.datasource.write.precombine.field': 'ts',
            'hoodie.upsert.shuffle.parallelism': 2, 
            'hoodie.insert.shuffle.parallelism': 2
        }

        deletes = list(map(lambda row: (row[0], row[1]), ds.collect()))
        hard_delete_df = self.spark.sparkContext.parallelize(deletes).toDF(['uuid', 'partitionpath']).withColumn('ts', lit(0.0))
        hard_delete_df.write.format("hudi").options(**hudi_hard_delete_options).mode("append").save(self.basePath)

        # run the same read query as above.
        roAfterDeleteViewDF = self.spark.read.format("hudi").load(self.basePath) 
        roAfterDeleteViewDF.createOrReplaceTempView("hudi_trips_snapshot")
        # fetch should return (total - 2) records
        total_count = self.spark.sql("SELECT uuid, partitionpath FROM hudi_trips_snapshot").count()
        print(f"total count: {total_count}")
        return hard_delete_df

    def insertOverwrite(self):
        print("Insert Overwrite")
        self.spark.read.format("hudi").load(self.basePath).select(["uuid","partitionpath"]).sort(["partitionpath", "uuid"]).show(n=100,truncate=False)
        inserts = self.spark._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(self.dataGen.generateInserts(10))
        df = self.spark.read.json(self.spark.sparkContext.parallelize(inserts, 2)).filter("partitionpath = 'americas/united_states/san_francisco'")
        hudi_insert_overwrite_options = {
            'hoodie.table.name': self.tableName,
            'hoodie.datasource.write.recordkey.field': 'uuid',
            'hoodie.datasource.write.partitionpath.field': 'partitionpath',
            'hoodie.datasource.write.table.name': self.tableName,
            'hoodie.datasource.write.operation': 'insert_overwrite',
            'hoodie.datasource.write.precombine.field': 'ts',
            'hoodie.upsert.shuffle.parallelism': 2,
            'hoodie.insert.shuffle.parallelism': 2
        }
        df.write.format("hudi").options(**hudi_insert_overwrite_options).mode("append").save(self.basePath)
        self.spark.read.format("hudi").load(self.basePath).select(["uuid","partitionpath"]).sort(["partitionpath", "uuid"]).show(n=100,truncate=False)
        return df

if __name__ == "__main__":
    random.seed(46474747)
    parser = argparse.ArgumentParser(description="Examples of various operations to perform on Hudi with PySpark",formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-t", "--table", action="store", required=True, help="the name of the table to create")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-p", "--package", action="store", help="the name of the hudi-spark-bundle package\n eg. \"org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0\"")
    group.add_argument("-j", "--jar", action="store", help="the full path to hudi-spark-bundle .jar file\n eg. \"[HUDI_BASE_PATH]/packaging/hudi-spark-bundle/target/hudi-spark-bundle[VERSION].jar\"")
    args = vars(parser.parse_args())
    package = args["package"]
    jar = args["jar"]
    if package != None:
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {package} pyspark-shell"
    elif "jar" != None:
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {jar} pyspark-shell"

    with tempfile.TemporaryDirectory() as tmpdirname:
        spark = sql.SparkSession \
            .builder \
            .appName("Hudi Spark basic example") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "512m") \
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .getOrCreate()
        ps = ExamplePySpark(spark,args["table"],tmpdirname)
        ps.runQuickstart()




