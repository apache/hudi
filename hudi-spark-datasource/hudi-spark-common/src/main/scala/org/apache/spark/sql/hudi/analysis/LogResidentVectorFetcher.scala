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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.avro.HoodieAvroReaderContext
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.expression.Predicate
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.log.InstantRange
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.storage.StorageConfiguration

import org.apache.avro.generic.{GenericFixed, IndexedRecord}
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Exact rerank fetch for MOR log-resident candidates.
 *
 * A fresh, uncompacted update stores its raw embedding in a log block, not the base Parquet file, so
 * the positional Parquet fetcher cannot materialize it and skips it. This fetcher fills that gap by
 * reusing Hudi's existing {@link HoodieFileGroupReader} + {@link HoodieAvroReaderContext} seam to read
 * the merged (base + log) file slice as Avro records, then scores only the log-resident candidate keys
 * with the SAME {@link VectorExactScorer} the positional path uses so exact-vs-brute-force stays equal.
 *
 * Serializable so each file group's merged read runs in its own Spark task: the cost of a merged read
 * scales with base+log size per touched file group, not with refineK, so it must not run on the driver.
 */
private[analysis] final class LogResidentVectorFetcher(
    storageConf: StorageConfiguration[_],
    metaClient: HoodieTableMetaClient,
    dataSchema: HoodieSchema,
    latestInstantTime: String,
    embeddingCol: String,
    outputSchema: StructType,
    scorer: VectorExactScorer) extends Serializable {

  /** Read a single merged file slice and return output rows for the requested log-resident keys. */
  def fetchSlice(
      partitionPath: String,
      fileSlice: FileSlice,
      candidateKeys: Set[String]): Seq[InternalRow] = {
    if (candidateKeys.isEmpty) {
      Seq.empty
    } else {
      fetchSliceNonEmpty(partitionPath, fileSlice, candidateKeys)
    }
  }

  private def fetchSliceNonEmpty(
      partitionPath: String,
      fileSlice: FileSlice,
      candidateKeys: Set[String]): Seq[InternalRow] = {
    val readerContext = new HoodieAvroReaderContext(
      storageConf, metaClient.getTableConfig,
      HOption.empty[InstantRange](), HOption.empty[Predicate](), new TypedProperties())
    val fileGroupReader: HoodieFileGroupReader[IndexedRecord] = HoodieFileGroupReader.builder()
      .withReaderContext(readerContext)
      .withHoodieTableMetaClient(metaClient)
      .withLatestCommitTime(latestInstantTime)
      .withLogFiles(fileSlice.getLogFiles)
      .withBaseFileOption(fileSlice.getBaseFile)
      .withPartitionPath(partitionPath)
      .withProps(new TypedProperties())
      .withDataSchema(dataSchema)
      .withRequestedSchema(dataSchema)
      .build()

    val rows = ArrayBuffer.empty[InternalRow]
    val remaining = scala.collection.mutable.Set(candidateKeys.toSeq: _*)
    val iter: ClosableIterator[HoodieRecord[IndexedRecord]] = fileGroupReader.getClosableHoodieRecordIterator
    try {
      while (iter.hasNext && remaining.nonEmpty) {
        val record = iter.next()
        val recordKey = record.getRecordKey
        if (remaining.contains(recordKey)) {
          remaining.remove(recordKey)
          // A NULL embedding (row upserted to NULL after indexing) is skipped, as brute force does.
          rows ++= materialize(record.getData)
        }
      }
    } finally {
      iter.close()
    }
    rows.toSeq
  }

  private def materialize(data: IndexedRecord): Option[InternalRow] = {
    val avroSchema = data.getSchema
    embeddingBytes(data, avroSchema).map(bytes => materialize(data, avroSchema, bytes))
  }

  private def materialize(data: IndexedRecord, avroSchema: org.apache.avro.Schema, vectorBytes: Array[Byte]): InternalRow = {
    val stored = storedDeserializer.deserialize(data).map(_.asInstanceOf[InternalRow]).orNull
    val values = new Array[Any](outputSchema.length)
    var ordinal = 0
    while (ordinal < outputSchema.length) {
      val source = outputSources(ordinal)
      if (source >= 0 && stored != null && !stored.isNullAt(source)) {
        values(ordinal) = stored.get(source, storedSchema(source).dataType)
      }
      ordinal += 1
    }
    values(outputSchema.fieldIndex(HoodieVectorSearchPlanBuilder.DISTANCE_COL)) = scorer.scoreBytes(vectorBytes)
    new GenericInternalRow(values)
  }

  // Output columns stored in the table, decoded by Spark's Avro deserializer (all types, including
  // nested and logical types). The embedding is never projected, matching the positional fetcher.
  private val storedSchema: StructType = StructType(outputSchema.fields.filter { field =>
    field.name != HoodieVectorSearchPlanBuilder.DISTANCE_COL && field.name != embeddingCol &&
      dataSchema.getField(field.name).isPresent
  })
  private val outputSources: Array[Int] = outputSchema.fields.map(field => storedSchema.fieldNames.indexOf(field.name))
  @transient private lazy val storedDeserializer: HoodieAvroDeserializer =
    SparkAdapterSupport.sparkAdapter.createAvroDeserializer(dataSchema, storedSchema)

  private def embeddingBytes(data: IndexedRecord, avroSchema: org.apache.avro.Schema): Option[Array[Byte]] = {
    val field = avroSchema.getField(embeddingCol)
    if (field == null) {
      throw new IllegalStateException(
        s"Vector column '$embeddingCol' is absent from the merged log-resident record for exact fetch.")
    }
    data.get(field.pos()) match {
      case null => None
      case bb: ByteBuffer =>
        val dup = bb.duplicate()
        val arr = new Array[Byte](dup.remaining())
        dup.get(arr)
        Some(arr)
      case fixed: GenericFixed => Some(fixed.bytes())
      case bytes: Array[Byte] => Some(bytes)
      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported embedding representation for log-resident exact fetch: ${other.getClass.getName}")
    }
  }
}
