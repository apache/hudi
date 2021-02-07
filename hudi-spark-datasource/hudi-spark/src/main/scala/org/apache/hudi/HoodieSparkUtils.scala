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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.SPARK_VERSION
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.execution.datasources.{FileStatusCache, InMemoryFileIndex}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._


object HoodieSparkUtils {

  def getMetaSchema: StructType = {
    StructType(HoodieRecord.HOODIE_META_COLUMNS.asScala.map(col => {
      StructField(col, StringType, nullable = true)
    }))
  }

  /**
   * This method copied from [[org.apache.spark.deploy.SparkHadoopUtil]].
   * [[org.apache.spark.deploy.SparkHadoopUtil]] becomes private since Spark 3.0.0 and hence we had to copy it locally.
   */
  def isGlobPath(pattern: Path): Boolean = {
    pattern.toString.exists("{}[]*?\\".toSet.contains)
  }

  /**
   * This method copied from [[org.apache.spark.deploy.SparkHadoopUtil]].
   * [[org.apache.spark.deploy.SparkHadoopUtil]] becomes private since Spark 3.0.0 and hence we had to copy it locally.
   */
  def globPath(fs: FileSystem, pattern: Path): Seq[Path] = {
    Option(fs.globStatus(pattern)).map { statuses =>
      statuses.map(_.getPath.makeQualified(fs.getUri, fs.getWorkingDirectory)).toSeq
    }.getOrElse(Seq.empty[Path])
  }

  /**
   * This method copied from [[org.apache.spark.deploy.SparkHadoopUtil]].
   * [[org.apache.spark.deploy.SparkHadoopUtil]] becomes private since Spark 3.0.0 and hence we had to copy it locally.
   */
  def globPathIfNecessary(fs: FileSystem, pattern: Path): Seq[Path] = {
    if (isGlobPath(pattern)) globPath(fs, pattern) else Seq(pattern)
  }

  /**
   * Checks to see whether input path contains a glob pattern and if yes, maps it to a list of absolute paths
   * which match the glob pattern. Otherwise, returns original path
   *
   * @param paths List of absolute or globbed paths
   * @param fs    File system
   * @return list of absolute file paths
   */
  def checkAndGlobPathIfNecessary(paths: Seq[String], fs: FileSystem): Seq[Path] = {
    paths.flatMap(path => {
      val qualified = new Path(path).makeQualified(fs.getUri, fs.getWorkingDirectory)
      val globPaths = globPathIfNecessary(fs, qualified)
      globPaths
    })
  }

  def createInMemoryFileIndex(sparkSession: SparkSession, globbedPaths: Seq[Path]): InMemoryFileIndex = {
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryFileIndex(sparkSession, globbedPaths, Map(), Option.empty, fileStatusCache)
  }

  def createRdd(df: DataFrame, structName: String, recordNamespace: String): RDD[GenericRecord] = {
    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, structName, recordNamespace)
    createRdd(df, avroSchema, structName, recordNamespace)
  }

  def createRdd(df: DataFrame, avroSchema: Schema, structName: String, recordNamespace: String)
  : RDD[GenericRecord] = {
    // Use the Avro schema to derive the StructType which has the correct nullability information
    val dataType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
    val encoder = RowEncoder.apply(dataType).resolveAndBind()
    val deserializer = HoodieSparkUtils.createRowSerDe(encoder)
    df.queryExecution.toRdd.map(row => deserializer.deserializeRow(row))
      .mapPartitions { records =>
        if (records.isEmpty) Iterator.empty
        else {
          val convertor = AvroConversionHelper.createConverterToAvro(dataType, structName, recordNamespace)
          records.map { x => convertor(x).asInstanceOf[GenericRecord] }
        }
      }
  }

  def createRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe = {
    // TODO remove Spark2RowSerDe if Spark 2.x support is dropped
    if (SPARK_VERSION.startsWith("2.")) {
      new Spark2RowSerDe(encoder)
    } else {
      new Spark3RowSerDe(encoder)
    }
  }
}
