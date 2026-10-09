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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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
 * Runs on the driver: the candidate set is bounded by refineK and the driver already holds the meta
 * client and resolved file slices, which avoids reconstructing them inside an executor closure.
 */
private[analysis] final class LogResidentVectorFetcher(
    storageConf: StorageConfiguration[_],
    metaClient: HoodieTableMetaClient,
    dataSchema: HoodieSchema,
    latestInstantTime: String,
    embeddingCol: String,
    outputSchema: StructType,
    scorer: VectorExactScorer) {

  import LogResidentVectorFetcher._

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
      candidateKeys: Set[String]): Seq[InternalRow] = {    val readerContext = new HoodieAvroReaderContext(
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
          rows += materialize(record.getData, recordKey)
        }
      }
    } finally {
      iter.close()
    }
    rows.toSeq
  }

  private def materialize(data: IndexedRecord, recordKey: String): InternalRow = {
    val avroSchema = data.getSchema
    val values = new Array[Any](outputSchema.length)
    var ordinal = 0
    while (ordinal < outputSchema.length) {
      val field = outputSchema.fields(ordinal)
      values(ordinal) = field.name match {
        case HoodieVectorSearchPlanBuilder.DISTANCE_COL =>
          scorer.scoreBytes(embeddingBytes(data, avroSchema))
        case name if name == embeddingCol =>
          // Match the positional materializer, which never projects the raw embedding into output.
          null
        case name =>
          Option(avroSchema.getField(name))
            .map(f => toCatalyst(field.dataType, data.get(f.pos())))
            .orNull
      }
      ordinal += 1
    }
    new GenericInternalRow(values)
  }

  private def embeddingBytes(data: IndexedRecord, avroSchema: org.apache.avro.Schema): Array[Byte] = {
    val field = avroSchema.getField(embeddingCol)
    if (field == null) {
      throw new IllegalStateException(
        s"Vector column '$embeddingCol' is absent from the merged log-resident record for exact fetch.")
    }
    data.get(field.pos()) match {
      case null =>
        throw new IllegalStateException(
          s"Vector column '$embeddingCol' was null in a merged log-resident record for exact fetch.")
      case bb: ByteBuffer =>
        val dup = bb.duplicate()
        val arr = new Array[Byte](dup.remaining())
        dup.get(arr)
        arr
      case fixed: GenericFixed => fixed.bytes()
      case bytes: Array[Byte] => bytes
      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported embedding representation for log-resident exact fetch: ${other.getClass.getName}")
    }
  }
}

private[analysis] object LogResidentVectorFetcher {

  /** Convert an Avro-decoded value to the catalyst representation for the given output field type. */
  private def toCatalyst(dataType: DataType, raw: Any): Any = {
    if (raw == null) {
      null
    } else {
      dataType match {
        case StringType => raw match {
          case bytes: Array[Byte] => UTF8String.fromBytes(bytes)
          case bb: ByteBuffer =>
            val dup = bb.duplicate()
            val arr = new Array[Byte](dup.remaining())
            dup.get(arr)
            UTF8String.fromBytes(arr)
          case other => UTF8String.fromString(String.valueOf(other))
        }
        case BinaryType => raw match {
          case bb: ByteBuffer =>
            val dup = bb.duplicate()
            val arr = new Array[Byte](dup.remaining())
            dup.get(arr)
            arr
          case bytes: Array[Byte] => bytes
          case fixed: GenericFixed => fixed.bytes()
          case other => throw unsupported(BinaryType, other)
        }
        case IntegerType => raw.asInstanceOf[java.lang.Number].intValue()
        case LongType => raw.asInstanceOf[java.lang.Number].longValue()
        case FloatType => raw.asInstanceOf[java.lang.Number].floatValue()
        case DoubleType => raw.asInstanceOf[java.lang.Number].doubleValue()
        case BooleanType => raw.asInstanceOf[java.lang.Boolean].booleanValue()
        case ByteType => raw.asInstanceOf[java.lang.Number].byteValue()
        case ShortType => raw.asInstanceOf[java.lang.Number].shortValue()
        case other => throw unsupported(other, raw)
      }
    }
  }

  private def unsupported(dataType: DataType, raw: Any): Throwable =
    new UnsupportedOperationException(
      s"Unsupported log-resident exact fetch output type $dataType for value ${raw.getClass.getName}")
}
