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

import org.apache.hudi.common.index.vector.VectorIndexMdtSearchUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordGlobalLocation}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData}
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter, RecordMaterializer}
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Locates record keys in one known base Parquet file, reading only the record-key column. */
private[analysis] final class ParquetRecordKeyLocator(conf: Configuration) extends Serializable {

  import ParquetRecordKeyLocator._

  def locate(
      filePath: String,
      candidates: Seq[VectorIndexMdtSearchUtils.ScoredPostingMatch]): LocateResult =
    if (candidates.isEmpty) {
      LocateResult(Seq.empty, LocateMetrics())
    } else {
      locateNonEmpty(filePath, candidates)
    }

  private def locateNonEmpty(
      filePath: String,
      candidates: Seq[VectorIndexMdtSearchUtils.ScoredPostingMatch]): LocateResult = {
    val path = new Path(filePath)
    val footer = ParquetFileReader.readFooter(conf, path)
    val fileSchema = footer.getFileMetaData.getSchema
    val keyField = fileSchema.getFields.asScala
      .find(_.getName == HoodieRecord.RECORD_KEY_METADATA_FIELD)
      .getOrElse(throw new IllegalArgumentException(
        s"Parquet file '$filePath' is missing '${HoodieRecord.RECORD_KEY_METADATA_FIELD}'."))
    val requestedSchema = new MessageType(fileSchema.getName, keyField)
    val wanted = candidates.map(candidate => candidate.getRecordKey -> candidate).toMap
    val remaining = mutable.Map(wanted.toSeq: _*)
    val located = mutable.ArrayBuffer.empty[VectorIndexMdtSearchUtils.ScoredPostingMatch]
    val rowGroups = footer.getBlocks.asScala.toIndexedSeq
    val rowGroupStarts = cumulativeStarts(rowGroups)
    var rowGroupsSelected = 0
    var rowsDecoded = 0L

    val reader = new ParquetFileReader(conf, path, footer)
    try {
      reader.setRequestedSchema(requestedSchema)
      rowGroups.zipWithIndex.foreach { case (block, ordinal) =>
        val keysForGroup = remaining.keySet.filter(key => mayContain(block, key))
        if (keysForGroup.nonEmpty) {
          rowGroupsSelected += 1
          val pageStore = reader.readRowGroup(ordinal)
          val recordReader = keyReader(fileSchema, requestedSchema, pageStore)
          var relativeRow = 0L
          while (relativeRow < block.getRowCount && remaining.nonEmpty) {
            val key = recordReader.read()
            rowsDecoded += 1
            remaining.remove(key).foreach { candidate =>
              val live = candidate.getLocation
              val trustedLocation = new HoodieRecordGlobalLocation(
                live.getPartitionPath,
                live.getInstantTime,
                live.getFileId,
                rowGroupStarts(ordinal) + relativeRow)
              located += candidate.withLocation(trustedLocation)
            }
            relativeRow += 1
          }
        }
      }
    } finally {
      reader.close()
    }
    LocateResult(
      located.toSeq,
      LocateMetrics(
        requested = candidates.size,
        located = located.size,
        rowGroupsTotal = rowGroups.size,
        rowGroupsSelected = rowGroupsSelected,
        rowsDecoded = rowsDecoded))
  }

  private def keyReader(
      fileSchema: MessageType,
      requestedSchema: MessageType,
      pageStore: PageReadStore) =
    new ColumnIOFactory().getColumnIO(requestedSchema, fileSchema)
      .getRecordReader(pageStore, new RecordKeyMaterializer)

  private def mayContain(block: BlockMetaData, key: String): Boolean = {
    val keyColumn = findKeyColumn(block)
    keyColumn.forall { column =>
      val stats = column.getStatistics
      if (stats == null || stats.isEmpty || !stats.hasNonNullValue) {
        true
      } else {
        val binaryKey = Binary.fromString(key)
        val min = stats.genericGetMin.asInstanceOf[Binary]
        val max = stats.genericGetMax.asInstanceOf[Binary]
        binaryKey.compareTo(min) >= 0 && binaryKey.compareTo(max) <= 0
      }
    }
  }

  private def findKeyColumn(block: BlockMetaData): Option[ColumnChunkMetaData] =
    block.getColumns.asScala.find(
      _.getPath.toDotString == HoodieRecord.RECORD_KEY_METADATA_FIELD)

  private final class RecordKeyMaterializer extends RecordMaterializer[String] {
    private var current: String = _
    private val root = new GroupConverter {
      private val keyConverter = new PrimitiveConverter {
        override def addBinary(value: Binary): Unit = current = value.toStringUsingUTF8
      }

      override def getConverter(fieldIndex: Int): Converter = keyConverter

      override def start(): Unit = current = null

      override def end(): Unit = {}
    }

    override def getCurrentRecord: String = current

    override def getRootConverter: GroupConverter = root
  }
}

private[analysis] object ParquetRecordKeyLocator {
  final case class LocateMetrics(
      requested: Int = 0,
      located: Int = 0,
      rowGroupsTotal: Int = 0,
      rowGroupsSelected: Int = 0,
      rowsDecoded: Long = 0L)

  final case class LocateResult(
      candidates: Seq[VectorIndexMdtSearchUtils.ScoredPostingMatch],
      metrics: LocateMetrics)

  private def cumulativeStarts(rowGroups: IndexedSeq[BlockMetaData]): IndexedSeq[Long] = {
    var next = 0L
    rowGroups.map { block =>
      val start = next
      next += block.getRowCount
      start
    }
  }
}
