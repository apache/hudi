/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import java.io.File
import java.nio.file.Paths
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.util
import scala.collection.JavaConverters

class TestHoodieSparkUtils {

  @Test
  def testGlobPaths(@TempDir tempDir: File): Unit = {
    val folders: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2").toUri)
    )

    val files: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file2").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file3").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file4").toUri)
    )

    folders.foreach(folder => new File(folder.toUri).mkdir())
    files.foreach(file => new File(file.toUri).createNewFile())

    var paths = Seq(tempDir.getAbsolutePath + "/*")
    var globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(folders.sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/*/*")
    globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(files.sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder1/*")
    globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(Seq(files(0), files(1)).sortWith(_.toString < _.toString),
      globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder2/*")
    globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(Seq(files(2), files(3)).sortWith(_.toString < _.toString),
      globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder1/*", tempDir.getAbsolutePath + "/folder2/*")
    globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(files.sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))
  }

  @Test
  def testCreateInMemoryIndex(@TempDir tempDir: File): Unit = {
    val spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate

    val folders: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2").toUri)
    )

    val files: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file2").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file3").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file4").toUri)
    )

    folders.foreach(folder => new File(folder.toUri).mkdir())
    files.foreach(file => new File(file.toUri).createNewFile())

    val index = HoodieSparkUtils.createInMemoryFileIndex(spark, Seq(folders(0), folders(1)))
    val indexedFilePaths = index.allFiles().map(fs => fs.getPath)
    assertEquals(files.sortWith(_.toString < _.toString), indexedFilePaths.sortWith(_.toString < _.toString))
    spark.stop()
  }

  @Test
  def testCreateRdd(@TempDir tempDir: File): Unit = {
    val spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate

    var schema = DataSourceTestUtils.getStructTypeExampleSchema
    var structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    var records = DataSourceTestUtils.generateRandomRows(5)
    var recordsSeq = convertRowListToSeq(records)
    var df1 = spark.createDataFrame(spark.sparkContext.parallelize(recordsSeq), structType)

    df1.collect().foreach(entry => System.out.println("Df row " + entry.mkString(",")))
    System.out.println("DF schema " + df1.schema.toString())

    var genRecRDD = HoodieSparkUtils.createRdd(df1, schema,"test_struct_name", "test_namespace")
    var genRecs = genRecRDD.collect()
    System.out.println("Schema of genRec 111 " + genRecs(0).getSchema.toString)
    genRecs.foreach(entry => System.out.println("Gen Rec 111 " + entry.toString))

    val evolSchema = DataSourceTestUtils.getStructTypeExampleEvolvedSchema
    val evolStructType = AvroConversionUtils.convertAvroSchemaToStructType(evolSchema)
    records = DataSourceTestUtils.generateRandomRowsEvolvedSchema(5)
    recordsSeq = convertRowListToSeq(records)
    val df3 = spark.createDataFrame(spark.sparkContext.parallelize(recordsSeq), evolStructType)

    genRecRDD = HoodieSparkUtils.createRdd(df1, evolSchema, "test_struct_name", "test_namespace")
    genRecs = genRecRDD.collect()
    System.out.println("Schema of genRec 222 " + genRecs(0).getSchema.toString)
    genRecs.foreach(entry => System.out.println("Gen Rec 222 " + entry.toString))

    val dfRows = AvroConversionUtils.createDataFrame(genRecRDD, evolSchema.toString, spark)
    dfRows.collect().foreach(entry => System.out.println("Df resultant row " + entry.mkString(",")))
    System.out.println("DF resultant schema " + df1.schema.toString())

    val finalGenRecRDD = HoodieSparkUtils.createRdd(dfRows, evolSchema, "test_struct_name","test_namespace")
    genRecs = finalGenRecRDD.collect()
    System.out.println("Schema of genRec 333 " + genRecs(0).getSchema.toString)
    genRecs.foreach(entry => System.out.println("Gen Rec 333 " + entry.toString))

  }

  def convertRowListToSeq(inputList: util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq
}
