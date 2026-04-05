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

package org.apache.hudi

import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.hudi.common.engine.RecordContext
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hudi.SparkAdapter

import java.io.IOException
import java.util.Properties
import scala.collection.mutable

trait SparkFileFormatInternalRecordContext extends BaseSparkInternalRecordContext {

  lazy val sparkAdapter: SparkAdapter = SparkAdapterSupport.sparkAdapter
  private val deserializerMap: mutable.Map[HoodieSchema, HoodieAvroDeserializer] = mutable.Map()
  private val serializerMap: mutable.Map[HoodieSchema, HoodieAvroSerializer] = mutable.Map()
  // Maps InternalRow instances (by identity) to their original Avro records when the Avro record's
  // schema differs from the BufferedRecord schema. This handles ExpressionPayload records whose
  // getInsertValue result carries the data schema (no meta fields) while the BufferedRecord
  // stores writeSchemaWithMetaFields. Returning the original Avro record from convertToAvroRecord
  // lets ExpressionPayload.combineAndGetUpdateValue decode bytes with the correct data schema.
  //
  // IMPORTANT INVARIANT: The identity link between InternalRow and GenericRecord must survive
  // from extractDataFromRecord (where the cache is populated) to convertToAvroRecord (where
  // it is consumed via remove()). Any operation that replaces the InternalRow object in between
  // — such as BufferedRecord.seal() (which calls InternalRow.copy()), replaceRecord(), or
  // project() — would break this link and cause fallback to schema-based serialization.
  //
  // Current safety: In the MOR read path (UpdateProcessor.handleNonDeletes), convertToAvroRecord
  // is called on the original InternalRow BEFORE seal() runs in super.handleNonDeletes().
  // In the COW write path (HoodieIndexUtils.inferPartitionPath), convertToAvroRecord is similarly
  // called before any record transformation.
  // LIFECYCLE: Entries are added in extractDataFromRecord and removed in convertToAvroRecord.
  // If a record is cached but never reaches convertToAvroRecord (e.g., SENTINEL records that
  // are filtered out), the entry leaks. This is bounded because: (1) SENTINEL records are rare,
  // and (2) this instance is scoped to a single FileGroup read and is GC'd afterwards.
  private val avroRecordByRow: java.util.IdentityHashMap[InternalRow, GenericRecord] =
    new java.util.IdentityHashMap[InternalRow, GenericRecord]()

  override def supportsParquetRowIndex: Boolean = {
    HoodieSparkUtils.gteqSpark3_5
  }

  /**
   * Extracts the engine-native record data from a [[HoodieRecord]].
   *
   * For Spark records the data is already an [[InternalRow]]. For records coming from
   * Avro-payload-based paths (e.g. [[org.apache.spark.sql.hudi.command.payload.ExpressionPayload]]
   * used in SQL MERGE INTO operations), the Avro representation is extracted via
   * [[HoodieRecord#toIndexedRecord]] and then deserialized to [[InternalRow]].
   */
  override def extractDataFromRecord(record: HoodieRecord[_], schema: HoodieSchema, properties: Properties): InternalRow = {
    val data = record.getData
    if (data == null || data.isInstanceOf[InternalRow]) {
      data.asInstanceOf[InternalRow]
    } else {
      // For records with non-InternalRow payloads (e.g. ExpressionPayload from SQL MERGE INTO),
      // extract the raw Avro record and convert it to InternalRow.
      try {
        val avroRecordOpt = record.toIndexedRecord(schema, properties)
        if (avroRecordOpt.isPresent) {
          val avroRecord = avroRecordOpt.get().getData.asInstanceOf[GenericRecord]
          val row = convertAvroRecord(avroRecord)
          // When the Avro record's schema differs from the BufferedRecord schema (e.g. ExpressionPayload
          // getInsertValue returns a data-schema record while schema is writeSchemaWithMetaFields),
          // cache the original Avro record keyed by the InternalRow identity. convertToAvroRecord will
          // return it directly, preserving the data schema so ExpressionPayload.combineAndGetUpdateValue
          // can decode the payload bytes with the correct PAYLOAD_RECORD_AVRO_SCHEMA.
          if (!avroRecord.getSchema.equals(schema.toAvroSchema)) {
            avroRecordByRow.put(row, avroRecord)
          }
          row
        } else null
      } catch {
        case e: IOException =>
          throw new HoodieException("Failed to extract data from record: " + record, e)
      }
    }
  }

  /**
   * Converts an Avro record, e.g., serialized in the log files, to an [[InternalRow]].
   *
   * @param avroRecord The Avro record.
   * @return An [[InternalRow]].
   */
  override def convertAvroRecord(avroRecord: IndexedRecord): InternalRow = {
    val schema = HoodieSchema.fromAvroSchema(avroRecord.getSchema)
    val structType = HoodieInternalRowUtils.getCachedSchema(schema)
    val deserializer = deserializerMap.getOrElseUpdate(schema, {
      sparkAdapter.createAvroDeserializer(schema, structType)
    })
    deserializer.deserialize(avroRecord).get.asInstanceOf[InternalRow]
  }

  override def convertToAvroRecord(record: InternalRow, schema: HoodieSchema): GenericRecord = {
    // If the InternalRow was produced from a payload (e.g. ExpressionPayload) whose Avro schema
    // differs from the BufferedRecord schema, return the original Avro record directly. This avoids
    // trying to serialize an InternalRow with the wrong schema and preserves the schema expected
    // by ExpressionPayload.combineAndGetUpdateValue (i.e., PAYLOAD_RECORD_AVRO_SCHEMA / data schema).
    val cached = avroRecordByRow.remove(record)
    if (cached != null) {
      return cached
    }
    val structType = HoodieInternalRowUtils.getCachedSchema(schema)
    val serializer = serializerMap.getOrElseUpdate(schema, {
      sparkAdapter.createAvroSerializer(structType, schema, schema.isNullable)
    })
    serializer.serialize(record).asInstanceOf[GenericRecord]
  }
}

object SparkFileFormatInternalRecordContext {
  // THREAD SAFETY NOTE: FIELD_ACCESSOR_INSTANCE is a static singleton used ONLY via
  // HoodieSparkRecord.isDeleteRecord(), which calls getFieldVal/getSealedRecord but
  // NEVER calls extractDataFromRecord or convertToAvroRecord. Therefore the singleton's
  // avroRecordByRow cache is always empty, and no thread-safety or memory-leak concern
  // applies. Per-task instances created via apply(tableConfig) are not shared across threads.
  // If new call sites are added for the singleton, this invariant must be re-evaluated.
  private val FIELD_ACCESSOR_INSTANCE = SparkFileFormatInternalRecordContext.apply()
  def getFieldAccessorInstance: RecordContext[InternalRow] = FIELD_ACCESSOR_INSTANCE
  def apply(): SparkFileFormatInternalRecordContext = new BaseSparkInternalRecordContext() with SparkFileFormatInternalRecordContext
  def apply(tableConfig: HoodieTableConfig): SparkFileFormatInternalRecordContext = new BaseSparkInternalRecordContext(tableConfig) with SparkFileFormatInternalRecordContext
}
