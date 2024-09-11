/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi

import org.apache.hudi.avro.HoodieAvroWriteSupport
import org.apache.hudi.client.SparkTaskContextSupplier
import org.apache.hudi.common.bloom.{BloomFilter, BloomFilterFactory}
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.config.HoodieStorageConfig.{BLOOM_FILTER_DYNAMIC_MAX_ENTRIES, BLOOM_FILTER_FPP_VALUE, BLOOM_FILTER_NUM_ENTRIES_VALUE, BLOOM_FILTER_TYPE}
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.util.Option
import org.apache.hudi.io.hadoop.HoodieAvroParquetWriter
import org.apache.hudi.io.storage.{HoodieIOFactory, HoodieParquetConfig}
import org.apache.hudi.storage.{HoodieStorage, StorageConfiguration, StoragePath}

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.{DataFrame, SQLContext}

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable._

object SparkHelpers {
  @throws[Exception]
  def skipKeysAndWriteNewFile(instantTime: String,
                              conf: StorageConfiguration[Configuration],
                              storage: HoodieStorage,
                              sourceFile: StoragePath,
                              destinationFile: StoragePath,
                              keysToSkip: Set[String]) {
    val sourceRecords = HoodieIOFactory.getIOFactory(storage)
      .getFileFormatUtils(HoodieFileFormat.PARQUET)
      .readAvroRecords(storage, sourceFile).asScala
    val schema: Schema = sourceRecords.head.getSchema
    val filter: BloomFilter = BloomFilterFactory.createBloomFilter(
      BLOOM_FILTER_NUM_ENTRIES_VALUE.defaultValue.toInt, BLOOM_FILTER_FPP_VALUE.defaultValue.toDouble,
      BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.defaultValue.toInt, BLOOM_FILTER_TYPE.defaultValue);
    val writeSupport: HoodieAvroWriteSupport[_] = new HoodieAvroWriteSupport(new AvroSchemaConverter(conf.unwrap()).convert(schema),
      schema, Option.of(filter), new Properties())
    val parquetConfig: HoodieParquetConfig[HoodieAvroWriteSupport[_]] =
      new HoodieParquetConfig(
        writeSupport,
        CompressionCodecName.GZIP,
        HoodieStorageConfig.PARQUET_BLOCK_SIZE.defaultValue.toInt,
        HoodieStorageConfig.PARQUET_PAGE_SIZE.defaultValue.toInt,
        HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.defaultValue.toInt,
        conf,
        HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue.toDouble,
        HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED.defaultValue)

    // Add current classLoad for config, if not will throw classNotFound of 'HoodieWrapperFileSystem'.
    conf.unwrap().setClassLoader(Thread.currentThread.getContextClassLoader)

    val writer = new HoodieAvroParquetWriter(destinationFile, parquetConfig, instantTime, new SparkTaskContextSupplier(), true)
    for (rec <- sourceRecords) {
      val key: String = rec.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString
      if (!keysToSkip.contains(key)) {

        writer.writeAvro(key, rec)
      }
    }
    writer.close
  }
}

/**
  * Bunch of Spark Shell/Scala stuff useful for debugging
  */
class SparkHelper(sqlContext: SQLContext, fs: FileSystem) {

  /**
    * Print keys from a file
    *
    * @param file
    */
  def printKeysFromFile(file: String) = {
    getRowKeyDF(file).collect().foreach(println(_))
  }

  /**
    *
    * @param file
    * @return
    */
  def getRowKeyDF(file: String): DataFrame = {
    sqlContext.read.parquet(file).select(s"`${HoodieRecord.RECORD_KEY_METADATA_FIELD}`")
  }

  /**
    * Does the rowKey actually exist in the file.
    *
    * @param rowKey
    * @param file
    * @return
    */
  def isFileContainsKey(rowKey: String, file: String): Boolean = {
    println(s"Checking $file for key $rowKey")
    val ff = getRowKeyDF(file).filter(s"`${HoodieRecord.RECORD_KEY_METADATA_FIELD}` = '$rowKey'")
    if (ff.count() > 0) true else false
  }

  /**
    * Number of keys in a given file
    *
    * @param file
    * @param sqlContext
    */
  def getKeyCount(file: String, sqlContext: org.apache.spark.sql.SQLContext) = {
    val keyCount = getRowKeyDF(file).collect().length
    println(keyCount)
    keyCount
  }

  /**
   *
   * Checks that all the keys in the file, have been added to the bloom filter
   * in the footer
   *
   * @param storage    [[HoodieStorage]] instance.
   * @param sqlContext Spark SQL context.
   * @param file       File path.
   * @return <pre>true</pre>  if all keys are added to the bloom filter;  <pre>false</pre>  otherwise.
   */
  def fileKeysAgainstBF(storage: HoodieStorage, sqlContext: SQLContext, file: String): Boolean = {
    val bf = HoodieIOFactory.getIOFactory(storage)
      .getFileFormatUtils(HoodieFileFormat.PARQUET)
      .readBloomFilterFromMetadata(storage, new StoragePath(file))
    val foundCount = sqlContext.parquetFile(file)
      .select(s"`${HoodieRecord.RECORD_KEY_METADATA_FIELD}`")
      .collect().count(r => !bf.mightContain(r.getString(0)))
    val totalCount = getKeyCount(file, sqlContext)
    println(s"totalCount: $totalCount, foundCount: $foundCount")
    totalCount == foundCount
  }

  def getDistinctKeyDF(paths: List[String]): DataFrame = {
    sqlContext.read.parquet(paths: _*).select(s"`${HoodieRecord.RECORD_KEY_METADATA_FIELD}`").distinct()
  }
}
