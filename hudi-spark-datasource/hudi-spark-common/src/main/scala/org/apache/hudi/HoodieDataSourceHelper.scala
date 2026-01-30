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

import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.storage.StoragePathInfo

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.{And, Filter, Or}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

object HoodieDataSourceHelper extends PredicateHelper with SparkAdapterSupport {


  /**
   * Wrapper for `buildReaderWithPartitionValues` of [[ParquetFileFormat]] handling [[ColumnarBatch]],
   * when Parquet's Vectorized Reader is used
   *
   * TODO move to HoodieBaseRelation, make private
   */
  private[hudi] def buildHoodieParquetReader(sparkSession: SparkSession,
                                             dataSchema: StructType,
                                             partitionSchema: StructType,
                                             requiredSchema: StructType,
                                             filters: Seq[Filter],
                                             options: Map[String, String],
                                             hadoopConf: Configuration,
                                             appendPartitionValues: Boolean = false,
                                             avroTableSchema: Schema): PartitionedFile => Iterator[InternalRow] = {
    val parquetFileFormat: ParquetFileFormat = sparkAdapter.createLegacyHoodieParquetFileFormat(appendPartitionValues, avroTableSchema).get
    val readParquetFile: PartitionedFile => Iterator[Any] = parquetFileFormat.buildReaderWithPartitionValues(
      sparkSession = sparkSession,
      dataSchema = dataSchema,
      partitionSchema = partitionSchema,
      requiredSchema = requiredSchema,
      filters = if (appendPartitionValues) getNonPartitionFilters(filters, dataSchema, partitionSchema) else filters,
      options = options,
      hadoopConf = hadoopConf
    )

    file: PartitionedFile => {
      val iter = readParquetFile(file)
      iter.flatMap {
        case r: InternalRow => Seq(r)
        case b: ColumnarBatch => b.rowIterator().asScala
      }
    }
  }

  def splitFiles(sparkSession: SparkSession,
                 file: StoragePathInfo,
                 partitionValues: InternalRow): Seq[PartitionedFile] = {
    val filePath = file.getPath
    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    (0L until file.getLength by maxSplitBytes).map { offset =>
      val remaining = file.getLength - offset
      val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
      sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(
        partitionValues, filePath, offset, size)
    }
  }

  trait AvroDeserializerSupport extends SparkAdapterSupport {
    protected val avroSchema: Schema
    protected val structTypeSchema: StructType

    private lazy val deserializer: HoodieAvroDeserializer =
      sparkAdapter.createAvroDeserializer(avroSchema, structTypeSchema)

    protected def deserialize(avroRecord: GenericRecord): InternalRow = {
      checkState(avroRecord.getSchema.getFields.size() == structTypeSchema.fields.length)
      deserializer.deserialize(avroRecord).get.asInstanceOf[InternalRow]
    }
  }

  def getNonPartitionFilters(filters: Seq[Filter], dataSchema: StructType, partitionSchema: StructType): Seq[Filter] = {
    filters.flatMap(f => {
      if (f.references.intersect(partitionSchema.fields.map(_.name)).nonEmpty) {
        extractPredicatesWithinOutputSet(f, dataSchema.fieldNames.toSet)
      } else {
        Some(f)
      }
    })
  }

  /**
   * Heavily adapted from {@see org.apache.spark.sql.catalyst.expressions.PredicateHelper#extractPredicatesWithinOutputSet}
   * Method is adapted to work with Filters instead of Expressions
   *
   * @return
   */
  def extractPredicatesWithinOutputSet(condition: Filter,
                                       outputSet: Set[String]): Option[Filter] = condition match {
    case And(left, right) =>
      val leftResultOptional = extractPredicatesWithinOutputSet(left, outputSet)
      val rightResultOptional = extractPredicatesWithinOutputSet(right, outputSet)
      (leftResultOptional, rightResultOptional) match {
        case (Some(leftResult), Some(rightResult)) => Some(And(leftResult, rightResult))
        case (Some(leftResult), None) => Some(leftResult)
        case (None, Some(rightResult)) => Some(rightResult)
        case _ => None
      }

    // The Or predicate is convertible when both of its children can be pushed down.
    // That is to say, if one/both of the children can be partially pushed down, the Or
    // predicate can be partially pushed down as well.
    //
    // Here is an example used to explain the reason.
    // Let's say we have
    // condition: (a1 AND a2) OR (b1 AND b2),
    // outputSet: AttributeSet(a1, b1)
    // a1 and b1 is convertible, while a2 and b2 is not.
    // The predicate can be converted as
    // (a1 OR b1) AND (a1 OR b2) AND (a2 OR b1) AND (a2 OR b2)
    // As per the logical in And predicate, we can push down (a1 OR b1).
    case Or(left, right) =>
      for {
        lhs <- extractPredicatesWithinOutputSet(left, outputSet)
        rhs <- extractPredicatesWithinOutputSet(right, outputSet)
      } yield Or(lhs, rhs)

    // Here we assume all the `Not` operators is already below all the `And` and `Or` operators
    // after the optimization rule `BooleanSimplification`, so that we don't need to handle the
    // `Not` operators here.
    case other =>
      if (other.references.toSet.subsetOf(outputSet)) {
        Some(other)
      } else {
        None
      }
  }
}
