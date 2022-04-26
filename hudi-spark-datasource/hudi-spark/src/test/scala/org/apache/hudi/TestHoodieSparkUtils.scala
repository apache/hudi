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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.exception.SchemaCompatibilityException
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.file.Paths
import scala.collection.JavaConverters

class TestHoodieSparkUtils {

  @Test
  def testGlobPaths(@TempDir tempDir: File): Unit = {
    val folders: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, ".hoodie").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, ".hoodie", "metadata").toUri)
    )

    val files: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file2").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file3").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2","file4").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, ".hoodie","metadata", "file5").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, ".hoodie","metadata", "file6").toUri)
    )

    folders.foreach(folder => new File(folder.toUri).mkdir())
    files.foreach(file => new File(file.toUri).createNewFile())

    var paths = Seq(tempDir.getAbsolutePath + "/*")
    var globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(folders.filterNot(entry => entry.toString.contains(".hoodie"))
      .sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/*/*")
    globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(files.filterNot(entry => entry.toString.contains(".hoodie"))
      .sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

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
    assertEquals(files.filterNot(entry => entry.toString.contains(".hoodie"))
      .sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))
  }

  @Test
  def testCreateRddSchemaEvol(): Unit = {
    val spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate

    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    var records = DataSourceTestUtils.generateRandomRows(5)
    var recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(recordsSeq), structType)

    var genRecRDD = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema))
    genRecRDD.collect()

    val evolSchema = DataSourceTestUtils.getStructTypeExampleEvolvedSchema
    records = DataSourceTestUtils.generateRandomRowsEvolvedSchema(5)
    recordsSeq = convertRowListToSeq(records)

    genRecRDD = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(evolSchema))
    genRecRDD.collect()

    // pass in evolved schema but with records serialized with old schema. should be able to convert with out any exception.
    // Before https://github.com/apache/hudi/pull/2927, this will throw exception.
    genRecRDD = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(evolSchema))
    val genRecs = genRecRDD.collect()
    // if this succeeds w/o throwing any exception, test succeeded.
    assertEquals(genRecs.size, 5)
    spark.stop()
  }

  @Test
  def testCreateRddWithNestedSchemas(): Unit = {
    val spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate

    val innerStruct1 = new StructType().add("innerKey","string",false).add("innerValue", "long", true)
    val structType1 = new StructType().add("key", "string", false)
      .add("nonNullableInnerStruct",innerStruct1,false).add("nullableInnerStruct",innerStruct1,true)
    val schema1 = AvroConversionUtils.convertStructTypeToAvroSchema(structType1, "test_struct_name", "test_namespace")
    val records1 = Seq(Row("key1", Row("innerKey1_1", 1L), Row("innerKey1_2", 2L)))

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(records1), structType1)
    val genRecRDD1 = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema1))
    assert(schema1.equals(genRecRDD1.collect()(0).getSchema))

    // create schema2 which has one addition column at the root level compared to schema1
    val structType2 = new StructType().add("key", "string", false)
      .add("nonNullableInnerStruct",innerStruct1,false).add("nullableInnerStruct",innerStruct1,true)
      .add("nullableInnerStruct2",innerStruct1,true)
    val schema2 = AvroConversionUtils.convertStructTypeToAvroSchema(structType2, "test_struct_name", "test_namespace")
    val records2 = Seq(Row("key2", Row("innerKey2_1", 2L), Row("innerKey2_2", 2L), Row("innerKey2_3", 2L)))
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(records2), structType2)
    val genRecRDD2 = HoodieSparkUtils.createRdd(df2, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema2))
    assert(schema2.equals(genRecRDD2.collect()(0).getSchema))

    // send records1 with schema2. should succeed since the new column is nullable.
    val genRecRDD3 = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema2))
    assert(genRecRDD3.collect()(0).getSchema.equals(schema2))
    genRecRDD3.foreach(entry => assertNull(entry.get("nullableInnerStruct2")))

    val innerStruct3 = new StructType().add("innerKey","string",false).add("innerValue", "long", true)
      .add("new_nested_col","string",true)

    // create a schema which has one additional nested column compared to schema1, which is nullable
    val structType4 = new StructType().add("key", "string", false)
      .add("nonNullableInnerStruct",innerStruct1,false).add("nullableInnerStruct",innerStruct3,true)

    val schema4 = AvroConversionUtils.convertStructTypeToAvroSchema(structType4, "test_struct_name", "test_namespace")
    val records4 = Seq(Row("key2", Row("innerKey2_1", 2L), Row("innerKey2_2", 2L, "new_nested_col_val1")))
    val df4 = spark.createDataFrame(spark.sparkContext.parallelize(records4), structType4)
    val genRecRDD4 = HoodieSparkUtils.createRdd(df4, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema4))
    assert(schema4.equals(genRecRDD4.collect()(0).getSchema))

    // convert batch 1 with schema4. should succeed.
    val genRecRDD5 = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
      org.apache.hudi.common.util.Option.of(schema4))
    assert(schema4.equals(genRecRDD4.collect()(0).getSchema))
    val genRec = genRecRDD5.collect()(0)
    val nestedRec : GenericRecord = genRec.get("nullableInnerStruct").asInstanceOf[GenericRecord]
    assertNull(nestedRec.get("new_nested_col"))
    assertNotNull(nestedRec.get("innerKey"))
    assertNotNull(nestedRec.get("innerValue"))

    val innerStruct4 = new StructType().add("innerKey","string",false).add("innerValue", "long", true)
      .add("new_nested_col","string",false)
    // create a schema which has one additional nested column compared to schema1, which is non nullable
    val structType6 = new StructType().add("key", "string", false)
      .add("nonNullableInnerStruct",innerStruct1,false).add("nullableInnerStruct",innerStruct4,true)

    val schema6 = AvroConversionUtils.convertStructTypeToAvroSchema(structType6, "test_struct_name", "test_namespace")
    // convert batch 1 with schema5. should fail since the missed out column is not nullable.
    try {
      val genRecRDD6 = HoodieSparkUtils.createRdd(df1, "test_struct_name", "test_namespace", true,
        org.apache.hudi.common.util.Option.of(schema6))
      genRecRDD6.collect()
      fail("createRdd should fail, because records don't have a column which is not nullable in the passed in schema")
    } catch {
      case e: Exception =>
        val cause = e.getCause
        assertTrue(cause.isInstanceOf[SchemaCompatibilityException])
        assertTrue(e.getMessage.contains("Unable to validate the rewritten record {\"innerKey\": \"innerKey1_2\", \"innerValue\": 2} against schema"))
    }
    spark.stop()
  }

  @Test
  def testGetRequiredSchema(): Unit = {
    val avroSchemaString = "{\"type\":\"record\",\"name\":\"record\"," +
    "\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
    "{\"name\":\"_hoodie_commit_seqno\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
    "{\"name\":\"_hoodie_record_key\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
    "{\"name\":\"_hoodie_partition_path\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
    "{\"name\":\"_hoodie_file_name\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
    "{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}," +
    "{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null}," +
    "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}," +
    "{\"name\":\"partition\",\"type\":[\"null\",\"string\"],\"default\":null}]}"

    val tableAvroSchema = new Schema.Parser().parse(avroSchemaString)

    val (requiredAvroSchema, requiredStructSchema, _) =
      HoodieSparkUtils.getRequiredSchema(tableAvroSchema, (Array("ts"), Array()))

    assertEquals("timestamp-millis",
      requiredAvroSchema.getField("ts").schema().getTypes.get(1).getLogicalType.getName)
    assertEquals(TimestampType, requiredStructSchema.fields(0).dataType)
  }

  def convertRowListToSeq(inputList: java.util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq
}
