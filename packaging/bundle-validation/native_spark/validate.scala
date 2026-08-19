/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val outputDir = "/tmp/native-spark-bundle"

// Force a real join rather than a broadcast, so the plan exercises Comet's join, shuffle and sort.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

def writeTable(name: String, numRecords: Int): String = {
  val path = "file:///tmp/hudi-bundles/tests/" + name
  val inserts = convertToStringList(new DataGenerator().generateInserts(numRecords)).asScala.toSeq
  spark.read.json(spark.sparkContext.parallelize(inserts, 2)).write.format("hudi").
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "ts").
    option(RECORDKEY_FIELD_OPT_KEY, "uuid").
    option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
    option(TABLE_NAME, name).
    mode(Overwrite).
    save(path)
  path
}

spark.read.format("hudi").load(writeTable("trips_native_1", 200)).createOrReplaceTempView("t1")
spark.read.format("hudi").load(writeTable("trips_native_2", 200)).createOrReplaceTempView("t2")

// Aggregate a data column, not just the partition column: a scan projecting no data columns
// reads as ReadSchema struct<> and Comet declines to bridge it into Arrow.
val query = spark.sql(
  "select t1.partitionpath, count(*) as c, sum(t1.fare) as s from t1 " +
  "join t2 on t1.partitionpath = t2.partitionpath group by t1.partitionpath")

// Comet does not recognize Hudi's file format and leaves the scan to Spark, but with
// spark.comet.convert.parquet.enabled it bridges Hudi's columnar output into Arrow and runs
// everything above the scan natively. Asserting on the plan matters because Comet degrades
// silently: a mis-relocated Comet or a missing libcomet.so still returns correct results.
val plan = query.queryExecution.executedPlan.toString
println("::warning::native bundle executed plan\n" + plan)
sc.parallelize(Seq(plan), 1).saveAsTextFile(outputDir + "/plan")
sc.parallelize(Seq(query.collect().length.toString), 1).saveAsTextFile(outputDir + "/count")

System.exit(0)
