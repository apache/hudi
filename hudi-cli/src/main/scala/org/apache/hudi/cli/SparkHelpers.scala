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

package org.apache.hudi.cli

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.avro.HoodieAvroWriteSupport
import org.apache.hudi.common.HoodieJsonPayload
import org.apache.hudi.common.bloom.filter.{BloomFilter, BloomFilterFactory}
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.config.{HoodieIndexConfig, HoodieStorageConfig}
import org.apache.hudi.io.storage.{HoodieParquetConfig, HoodieParquetWriter}
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.JavaConversions._
import scala.collection.mutable._


object SparkHelpers {
  @throws[Exception]
  def skipKeysAndWriteNewFile(commitTime: String, fs: FileSystem, sourceFile: Path, destinationFile: Path, keysToSkip: Set[String]) {
    val sourceRecords = ParquetUtils.readAvroRecords(fs.getConf, sourceFile)
    val schema: Schema = sourceRecords.get(0).getSchema
    val filter: BloomFilter = BloomFilterFactory.createBloomFilter(HoodieIndexConfig.DEFAULT_BLOOM_FILTER_NUM_ENTRIES.toInt, HoodieIndexConfig.DEFAULT_BLOOM_FILTER_FPP.toDouble,
      HoodieIndexConfig.DEFAULT_HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES.toInt, HoodieIndexConfig.DEFAULT_BLOOM_INDEX_FILTER_TYPE);
    val writeSupport: HoodieAvroWriteSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema, filter)
    val parquetConfig: HoodieParquetConfig = new HoodieParquetConfig(writeSupport, CompressionCodecName.GZIP, HoodieStorageConfig.DEFAULT_PARQUET_BLOCK_SIZE_BYTES.toInt, HoodieStorageConfig.DEFAULT_PARQUET_PAGE_SIZE_BYTES.toInt, HoodieStorageConfig.DEFAULT_PARQUET_FILE_MAX_BYTES.toInt, fs.getConf, HoodieStorageConfig.DEFAULT_STREAM_COMPRESSION_RATIO.toDouble)
    val writer = new HoodieParquetWriter[HoodieJsonPayload, IndexedRecord](commitTime, destinationFile, parquetConfig, schema)
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
    * @param conf
    * @param sqlContext
    * @param file
    * @return
    */
  def fileKeysAgainstBF(conf: Configuration, sqlContext: SQLContext, file: String): Boolean = {
    val bf = ParquetUtils.readBloomFilterFromParquetMetadata(conf, new Path(file))
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
