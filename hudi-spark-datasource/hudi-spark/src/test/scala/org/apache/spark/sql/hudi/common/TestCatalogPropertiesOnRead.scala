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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.{DataSourceReadOptions, HoodieBaseRelation, HoodieFileIndex}
import org.apache.hudi.common.table.HoodieTableConfig

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import java.io.File

/**
 * Catalog TBLPROPERTIES merged into a read of a table whose hoodie.properties records no database
 * must come from the catalog entry the read resolved to, never from a same-named table in the
 * session's current database.
 */
class TestCatalogPropertiesOnRead extends HoodieSparkSqlTestBase {

  private val Marker = "hoodie.test.catalog.marker"

  /** Writes a table with no hoodie.database.name, registers it as db1.<name>, and a decoy default.<name> elsewhere. */
  private def withKeylessTableAndDecoy(f: (String, String) => Unit): Unit = withTempDir { dir =>
    val name = generateTableName
    val path = new File(dir, "real").getCanonicalPath
    spark.range(3).selectExpr("cast(id as int) id", "'x' v", "cast(1 as bigint) ts").write.format("hudi")
      .option("hoodie.table.name", name)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "")
      .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .mode("overwrite").save(path)
    spark.sql("CREATE DATABASE IF NOT EXISTS db1")
    spark.sql(s"CREATE TABLE db1.$name USING hudi LOCATION '$path' TBLPROPERTIES ('$Marker'='db1')")
    spark.sql(s"CREATE TABLE default.$name (id int, v string, ts long) USING hudi LOCATION '${new File(dir, "decoy").getCanonicalPath}' " +
      s"TBLPROPERTIES (primaryKey='id', preCombineField='ts', '$Marker'='decoy')")
    val props = scala.io.Source.fromFile(new File(path, ".hoodie/hoodie.properties"))
    try assert(!props.getLines().exists(_.startsWith(HoodieTableConfig.DATABASE_NAME.key)), "fixture must record no database")
    finally props.close()
    f(name, path)
  }

  private def fileIndexOf(df: DataFrame): HoodieFileIndex = df.queryExecution.optimizedPlan.collectFirst {
    case LogicalRelation(r: HadoopFsRelation, _, _, _) => r.location.asInstanceOf[HoodieFileIndex]
    case LogicalRelation(r: HoodieBaseRelation, _, _, _) => r.fileIndex
  }.get

  /** The marker getConfigProperties merges for this read, recomputed with the inputs HoodieFileIndex used. */
  private def mergedMarker(s: SparkSession, df: DataFrame): Option[String] = {
    val fi = fileIndexOf(df)
    val props = HoodieFileIndex.getConfigProperties(s, fi.options, fi.metaClient.getTableConfig, Some(fi.metaClient.getBasePath))
    Option(props.getProperty(Marker))
  }

  private def hoodieCatalogSession(): SparkSession = spark.newSession()

  private def plainCatalogSession(): SparkSession = {
    val s = spark.newSession()
    s.conf.unset("spark.sql.catalog.spark_catalog")
    s
  }

  test("qualified read through HoodieCatalog merges the resolved entry, not a same-named table in the current database") {
    withKeylessTableAndDecoy { (name, _) =>
      val s = hoodieCatalogSession()
      val df = s.sql(s"SELECT * FROM db1.$name")
      assert(df.count() == 3)
      assert(fileIndexOf(df).options.get(DataSourceReadOptions.CATALOG_TABLE_DATABASE.key).contains("db1"))
      assert(mergedMarker(s, df).contains("db1"))
    }
  }

  test("qualified read through HoodieCatalog with schema-on-read merges the resolved entry") {
    withKeylessTableAndDecoy { (name, _) =>
      val s = hoodieCatalogSession()
      s.conf.set("hoodie.schema.on.read.enable", "true")
      val df = s.sql(s"SELECT * FROM db1.$name")
      assert(df.count() == 3)
      assert(mergedMarker(s, df).contains("db1"))
    }
  }

  test("qualified read without HoodieCatalog skips a same-named table at another location") {
    withKeylessTableAndDecoy { (name, _) =>
      val s = plainCatalogSession()
      val df = s.sql(s"SELECT * FROM db1.$name")
      assert(df.count() == 3)
      assert(!fileIndexOf(df).options.contains(DataSourceReadOptions.CATALOG_TABLE_DATABASE.key))
      assert(mergedMarker(s, df).isEmpty)
    }
  }

  test("unqualified read after USE keeps merging the table's own catalog properties") {
    withKeylessTableAndDecoy { (name, _) =>
      Seq(hoodieCatalogSession(), plainCatalogSession()).foreach { s =>
        s.sql("USE db1")
        val df = s.sql(s"SELECT * FROM $name")
        assert(df.count() == 3)
        assert(mergedMarker(s, df).contains("db1"))
      }
    }
  }

  test("path read merges only an entry in the current database that points at the same location") {
    withKeylessTableAndDecoy { (_, path) =>
      val s = hoodieCatalogSession()
      val fromDefault = s.read.format("hudi").load(path)
      assert(fromDefault.count() == 3)
      assert(mergedMarker(s, fromDefault).isEmpty)

      s.sql("USE db1")
      val fromDb1 = s.read.format("hudi").load(path)
      assert(mergedMarker(s, fromDb1).contains("db1"))
    }
  }
}
