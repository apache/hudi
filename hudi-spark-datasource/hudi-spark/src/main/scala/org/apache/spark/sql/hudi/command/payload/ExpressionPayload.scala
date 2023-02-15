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

package org.apache.spark.sql.hudi.command.payload

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.hudi.AvroConversionUtils.convertAvroSchemaToStructType
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.avro.AvroSchemaUtils.isNullable
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.avro.HoodieAvroUtils.bytesToAvro
import org.apache.hudi.common.model.BaseAvroPayload.isDeleteRecord
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodiePayloadProps, HoodieRecord}
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.{BinaryUtil, ValidationUtils, Option => HOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{KryoSerializer, SerializerInstance}
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Projection, SafeProjection}
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.{SparkConf, SparkEnv}

import java.nio.ByteBuffer
import java.util.function.{Function, Supplier}
import java.util.{Base64, Properties}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * A HoodieRecordPayload for MergeIntoHoodieTableCommand.
 * It will execute the condition and assignments expression in the
 * match and not-match actions and compute the final record to write.
 *
 * If there is no condition match the record, ExpressionPayload will return
 * a HoodieWriteHandle.IGNORE_RECORD, and the write handles will ignore this record.
 *
 * NOTE: Please note that, ctor parameter SHOULD NOT be used w/in the class body as
 *       otherwise Scala will instantiate them as fields making whole [[ExpressionPayload]]
 *       non-serializable. As an additional hedge, these are annotated as [[transient]] to
 *       prevent this from happening.
 */
class ExpressionPayload(@transient record: GenericRecord,
                        @transient orderingVal: Comparable[_])
  extends DefaultHoodieRecordPayload(record, orderingVal) with Logging {

  def this(recordOpt: HOption[GenericRecord]) {
    this(recordOpt.orElse(null), 0)
  }

  override def combineAndGetUpdateValue(currentValue: IndexedRecord,
                                        schema: Schema): HOption[IndexedRecord] = {
    throw new IllegalStateException(s"Should not call this method for ${getClass.getCanonicalName}")
  }

  override def getInsertValue(schema: Schema): HOption[IndexedRecord] = {
    throw new IllegalStateException(s"Should not call this method for ${getClass.getCanonicalName}")
  }

  override def combineAndGetUpdateValue(targetRecord: IndexedRecord,
                                        schema: Schema,
                                        properties: Properties): HOption[IndexedRecord] = {
    val recordSchema = getRecordSchema(properties)

    val sourceRecord = bytesToAvro(recordBytes, recordSchema)
    val joinedRecord = joinRecord(sourceRecord, targetRecord)

    processMatchedRecord(ConvertibleRecord(joinedRecord), Some(targetRecord), properties)
  }

  override def canProduceSentinel: Boolean = true

  /**
   * Process the matched record. Firstly test if the record matched any of the update-conditions,
   * if matched, return the update assignments result. Secondly, test if the record matched
   * delete-condition, if matched then return a delete record. Finally if no condition matched,
   * return a [[HoodieRecord.SENTINEL]] which will be ignored by HoodieWriteHandle.
   *
   * @param inputRecord  The input record to process.
   * @param targetRecord The origin exist record.
   * @param properties   The properties.
   * @return The result of the record to update or delete.
   */
  private def processMatchedRecord(inputRecord: ConvertibleRecord,
                                   targetRecord: Option[IndexedRecord],
                                   properties: Properties): HOption[IndexedRecord] = {
    // Process update
    val updateConditionAndAssignmentsText =
      properties.get(ExpressionPayload.PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS)

    checkState(updateConditionAndAssignmentsText != null,
      s"${ExpressionPayload.PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS} have to be set")

    var resultRecordOpt: HOption[IndexedRecord] = null

    // Get the Evaluator for each condition and update assignments.
    val updateConditionAndAssignments =
      getEvaluator(updateConditionAndAssignmentsText.toString, inputRecord.asAvro.getSchema)

    for ((conditionEvaluator, assignmentEvaluator) <- updateConditionAndAssignments
         if resultRecordOpt == null) {
      val conditionEvalResult = conditionEvaluator.apply(inputRecord.asRow)
        .get(0, BooleanType)
        .asInstanceOf[Boolean]

      // If the update condition matched  then execute assignment expression
      // to compute final record to update. We will return the first matched record.
      if (conditionEvalResult) {
        val writerSchema = getWriterSchema(properties)
        val resultingRow = assignmentEvaluator.apply(inputRecord.asRow)
        lazy val resultingAvroRecord = getAvroSerializerFor(writerSchema)
          .serialize(resultingRow)
          .asInstanceOf[GenericRecord]

        if (targetRecord.isEmpty || needUpdatingPersistedRecord(targetRecord.get, resultingAvroRecord, properties)) {
          resultRecordOpt = HOption.of(resultingAvroRecord)
        } else {
          // if the PreCombine field value of targetRecord is greater
          // than the new incoming record, just keep the old record value.
          resultRecordOpt = HOption.of(targetRecord.get)
        }
      }
    }

    if (resultRecordOpt == null) {
      // Process delete
      val deleteConditionText = properties.get(ExpressionPayload.PAYLOAD_DELETE_CONDITION)
      if (deleteConditionText != null) {
        val (deleteConditionEvaluator, _) = getEvaluator(deleteConditionText.toString, inputRecord.asAvro.getSchema).head
        val deleteConditionEvalResult = deleteConditionEvaluator.apply(inputRecord.asRow)
          .get(0, BooleanType)
          .asInstanceOf[Boolean]
        if (deleteConditionEvalResult) {
          resultRecordOpt = HOption.empty()
        }
      }
    }
    if (resultRecordOpt == null) {
      // If there is no condition matched, just filter this record.
      // here we return a IGNORE_RECORD, HoodieMergeHandle will not handle it.
      HOption.of(HoodieRecord.SENTINEL)
    } else {
      resultRecordOpt
    }
  }

  /**
   * Holding wrapper record providing for lazy conversion into Catalyst's [[InternalRow]] from Avro
   *
   * NOTE: This wrapper is necessary to avoid converting Avro record into [[InternalRow]]
   *       multiple times for different expression evaluation invocations
   */
  case class ConvertibleRecord(private val avro: GenericRecord) extends Logging {
    private lazy val row: InternalRow = getAvroDeserializerFor(avro.getSchema).deserialize(avro) match {
      case Some(row) => row.asInstanceOf[InternalRow]
      case None =>
        logError(s"Failed to deserialize Avro record `${avro.toString}` as Catalyst row")
        throw new HoodieException("Failed to deserialize Avro record as Catalyst row")
    }

    def asAvro = avro
    def asRow = row
  }

  /**
   * Process the not-matched record. Test if the record matched any of insert-conditions,
   * if matched then return the result of insert-assignment. Or else return a
   * [[HoodieRecord.SENTINEL]] which will be ignored by HoodieWriteHandle.
   *
   * @param inputRecord The input record to process.
   * @param properties  The properties.
   * @return The result of the record to insert.
   */
  private def processNotMatchedRecord(inputRecord: ConvertibleRecord, properties: Properties): HOption[IndexedRecord] = {
    val insertConditionAndAssignmentsText: String =
      properties.get(ExpressionPayload.PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS).toString
    // Get the evaluator for each condition and insert assignment.
    val insertConditionAndAssignments =
      ExpressionPayload.getEvaluator(insertConditionAndAssignmentsText, inputRecord.asAvro.getSchema)
    var resultRecordOpt: HOption[IndexedRecord] = null
    for ((conditionEvaluator, assignmentEvaluator) <- insertConditionAndAssignments
         if resultRecordOpt == null) {
      val conditionEvalResult = conditionEvaluator.apply(inputRecord.asRow)
        .get(0, BooleanType)
        .asInstanceOf[Boolean]
      // If matched the insert condition then execute the assignment expressions to compute the
      // result record. We will return the first matched record.
      if (conditionEvalResult) {
        val writerSchema = getWriterSchema(properties)
        val resultingRow = assignmentEvaluator.apply(inputRecord.asRow)
        val resultingAvroRecord = getAvroSerializerFor(writerSchema)
          .serialize(resultingRow)
          .asInstanceOf[GenericRecord]

        resultRecordOpt = HOption.of(resultingAvroRecord)
      }
    }
    if (resultRecordOpt != null) {
      resultRecordOpt
    } else {
      // If there is no condition matched, just filter this record.
      // Here we return a IGNORE_RECORD, HoodieCreateHandle will not handle it.
      HOption.of(HoodieRecord.SENTINEL)
    }
  }

  override def isDeleted(schema: Schema, props: Properties): Boolean = {
    val deleteConditionText = props.get(ExpressionPayload.PAYLOAD_DELETE_CONDITION)
    val isUpdateRecord = props.getProperty(HoodiePayloadProps.PAYLOAD_IS_UPDATE_RECORD_FOR_MOR, "false").toBoolean
    val isDeleteOnCondition= if (isUpdateRecord && deleteConditionText != null) {
      !getInsertValue(schema, props).isPresent
    } else false

    isDeletedRecord || isDeleteOnCondition
  }

  override def getInsertValue(schema: Schema, properties: Properties): HOption[IndexedRecord] = {
    val recordSchema = getRecordSchema(properties)
    val incomingRecord = ConvertibleRecord(bytesToAvro(recordBytes, recordSchema))

    if (isDeleteRecord(incomingRecord.asAvro)) {
      HOption.empty[IndexedRecord]()
    } else if (isMORTable(properties)) {
      // For the MOR table, both the matched and not-matched record will step into the getInsertValue() method.
      // We call the processMatchedRecord() method if current is a Update-Record to process
      // the matched record. Or else we call processNotMatchedRecord() method to process the not matched record.
      val isUpdateRecord = properties.getProperty(HoodiePayloadProps.PAYLOAD_IS_UPDATE_RECORD_FOR_MOR, "false").toBoolean
      if (isUpdateRecord) {
        processMatchedRecord(incomingRecord, Option.empty, properties)
      } else {
        processNotMatchedRecord(incomingRecord, properties)
      }
    } else {
      // For COW table, only the not-matched record will step into the getInsertValue method, So just call
      // the processNotMatchedRecord() here.
      processNotMatchedRecord(incomingRecord, properties)
    }
  }

  private def isMORTable(properties: Properties): Boolean = {
    properties.getProperty(TABLE_TYPE.key, null) == MOR_TABLE_TYPE_OPT_VAL
  }

  private def convertToRecord(values: Array[AnyRef], schema: Schema): GenericRecord = {
    assert(values.length == schema.getFields.size())
    val writeRecord = new GenericData.Record(schema)
    for (i <- values.indices) {
      writeRecord.put(i, values(i))
    }
    writeRecord
  }

  /**
   * Join the source record with the target record.
   *
   * @return
   */
  private def joinRecord(sourceRecord: IndexedRecord, targetRecord: IndexedRecord): GenericRecord = {
    val leftSchema = sourceRecord.getSchema
    val joinSchema = getMergedSchema(leftSchema, targetRecord.getSchema)

    // TODO rebase onto JoinRecord
    val values = new ArrayBuffer[AnyRef](joinSchema.getFields.size())
    for (i <- 0 until joinSchema.getFields.size()) {
      val value = if (i < leftSchema.getFields.size()) {
        sourceRecord.get(i)
      } else { // skip meta field
        targetRecord.get(i - leftSchema.getFields.size() + HoodieRecord.HOODIE_META_COLUMNS.size())
      }
      values += value
    }
    convertToRecord(values.toArray, joinSchema)
  }
}

object ExpressionPayload {

  /**
   * Property for pass the merge-into delete clause condition expression.
   */
  val PAYLOAD_DELETE_CONDITION = "hoodie.payload.delete.condition"

  /**
   * Property for pass the merge-into update clauses's condition and assignments.
   */
  val PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS = "hoodie.payload.update.condition.assignments"

  /**
   * Property for pass the merge-into insert clauses's condition and assignments.
   */
  val PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS = "hoodie.payload.insert.condition.assignments"

  /**
   * Property holding record's original (Avro) schema
   */
  val PAYLOAD_RECORD_AVRO_SCHEMA = "hoodie.payload.record.schema"

  /**
   * NOTE: PLEASE READ CAREFULLY
   *       Spark's [[SafeProjection]] are NOT thread-safe hence cache is scoped
   *       down to be thread-local to support the multi-threaded executors (like
   *       [[BoundedInMemoryQueueExecutor]], [[DisruptorExecutor]])
   *
   * To avoid compiling projections for Merge Into expressions for every record these
   * are cached under a key of
   * <ol>
   *    <li>Expression's (textual) representation</li>
   *    <li>Expected input-schema</li>
   * </ol>
   *
   * NOTE: Schema is required b/c these cache is static and might be shared by multiple
   *       executed statements w/in a single Spark session
   */
  private val projectionsCache = ThreadLocal.withInitial(
    new Supplier[Cache[(String, Schema), Seq[(Projection, Projection)]]] {
      override def get(): Cache[(String, Schema), Seq[(Projection, Projection)]] = {
        Caffeine.newBuilder()
          .maximumSize(1024)
          .build[(String, Schema), Seq[(Projection, Projection)]]()
      }
    })

  /**
   * NOTE: PLEASE READ CAREFULLY
   *       Spark's [[AvroDeserializer]] are NOT thread-safe hence cache is scoped
   *       down to be thread-local to support the multi-threaded executors (like
   *       [[BoundedInMemoryQueueExecutor]], [[DisruptorExecutor]])
   */
  private val avroDeserializerCache = ThreadLocal.withInitial(
    new Supplier[Cache[Schema, HoodieAvroDeserializer]] {
      override def get(): Cache[Schema, HoodieAvroDeserializer] =
        Caffeine.newBuilder()
          .maximumSize(16).build[Schema, HoodieAvroDeserializer]()
    }
  )

  /**
   * NOTE: PLEASE READ CAREFULLY
   *       Spark's [[AvroSerializer]] are NOT thread-safe hence cache is scoped
   *       down to be thread-local to support the multi-threaded executors (like
   *       [[BoundedInMemoryQueueExecutor]], [[DisruptorExecutor]])
   */
  private val avroSerializerCache = ThreadLocal.withInitial(
    new Supplier[Cache[Schema, HoodieAvroSerializer]] {
      override def get(): Cache[Schema, HoodieAvroSerializer] =
        Caffeine.newBuilder()
          .maximumSize(16).build[Schema, HoodieAvroSerializer]()
    }
  )

  private val schemaCache = Caffeine.newBuilder()
    .maximumSize(16).build[String, Schema]()

  private val mergedSchemaCache = Caffeine.newBuilder()
    .maximumSize(16).build[(Schema, Schema), Schema]()

  private def parseSchema(schemaStr: String): Schema = {
    schemaCache.get(schemaStr,
      new Function[String, Schema] {
        override def apply(t: String): Schema = new Schema.Parser().parse(t)
    })
  }

  private def getRecordSchema(props: Properties) = {
    ValidationUtils.checkArgument(props.containsKey(PAYLOAD_RECORD_AVRO_SCHEMA),
      s"Missing ${PAYLOAD_RECORD_AVRO_SCHEMA} property")
    parseSchema(props.getProperty(PAYLOAD_RECORD_AVRO_SCHEMA))
  }

  private def getWriterSchema(props: Properties): Schema = {
    ValidationUtils.checkArgument(props.containsKey(HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key),
      s"Missing ${HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key} property")
    parseSchema(props.getProperty(HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE.key))
  }

  private def getAvroDeserializerFor(schema: Schema) = {
    avroDeserializerCache.get()
      .get(schema, new Function[Schema, HoodieAvroDeserializer] {
        override def apply(t: Schema): HoodieAvroDeserializer =
          sparkAdapter.createAvroDeserializer(schema, convertAvroSchemaToStructType(schema))
      })
  }

  private def getAvroSerializerFor(schema: Schema) = {
    avroSerializerCache.get()
      .get(schema, new Function[Schema, HoodieAvroSerializer] {
        override def apply(t: Schema): HoodieAvroSerializer =
          sparkAdapter.createAvroSerializer(convertAvroSchemaToStructType(schema), schema, isNullable(schema))
      })
  }

  /**
   * Do the CodeGen for each condition and assignment expressions.We will projectionsCache it to reduce
   * the compile time for each method call.
   */
  private def getEvaluator(serializedConditionAssignments: String,
                           inputSchema: Schema): Seq[(Projection, Projection)] = {
    projectionsCache.get()
      .get((serializedConditionAssignments, inputSchema),
        new Function[(String, Schema), Seq[(Projection, Projection)]] {
          override def apply(key: (String, Schema)): Seq[(Projection, Projection)] = {
            val (encodedConditionalAssignments, _) = key
            val serializedBytes = Base64.getDecoder.decode(encodedConditionalAssignments)
            val conditionAssignments = Serializer.toObject(serializedBytes)
              .asInstanceOf[Map[Expression, Seq[Expression]]]
            conditionAssignments.toSeq.map {
              case (condition, assignments) =>
                // NOTE: We reuse Spark's [[Projection]]s infra for actual evaluation of the
                //       expressions, allowing us to execute arbitrary expression against input
                //       [[InternalRow]] producing another [[InternalRow]] as an outcome
                val conditionEvaluator = SafeProjection.create(Seq(condition))
                val assignmentEvaluator = SafeProjection.create(assignments)

                conditionEvaluator -> assignmentEvaluator
            }
          }
        })
  }

  private def getMergedSchema(source: Schema, target: Schema): Schema = {
    mergedSchemaCache.get((source, target), new Function[(Schema, Schema), Schema] {
      override def apply(t: (Schema, Schema)): Schema = {
        val rightSchema = HoodieAvroUtils.removeMetadataFields(t._2)
        mergeSchema(t._1, rightSchema)
      }
    })
  }

  private def mergeSchema(a: Schema, b: Schema): Schema = {
    val mergedFields =
      a.getFields.asScala.map(field =>
        new Schema.Field("a_" + field.name,
          field.schema, field.doc, field.defaultVal, field.order)) ++
        b.getFields.asScala.map(field =>
          new Schema.Field("b_" + field.name,
            field.schema, field.doc, field.defaultVal, field.order))
    Schema.createRecord(a.getName, a.getDoc, a.getNamespace, a.isError, mergedFields.asJava)
  }


  /**
   * This object differs from Hudi's generic [[SerializationUtils]] in its ability to serialize
   * Spark's internal structures (various [[Expression]]s)
   *
   * For that purpose we re-use Spark's [[KryoSerializer]] instance sharing configuration
   * with enclosing [[SparkEnv]]. This is necessary to make sure that this particular instance of Kryo
   * user for serialization of Spark's internal structures (like [[Expression]]s) is configured
   * appropriately (class-loading, custom serializers, etc)
   *
   * TODO rebase on Spark's SerializerSupport
   */
  private[hudi] object Serializer {

    // NOTE: This is only Spark >= 3.0
    private val KRYO_USE_POOL_CONFIG_KEY = "spark.kryo.pool"

    private lazy val conf = {
      val conf = Option(SparkEnv.get)
        // To make sure we're not modifying existing environment's [[SparkConf]]
        // we're cloning it here
        .map(_.conf.clone)
        .getOrElse(new SparkConf)
      // This serializer is configured as thread-local, hence there's no need for
      // pooling
      conf.set(KRYO_USE_POOL_CONFIG_KEY, "false")
      conf
    }

    private val SERIALIZER_THREAD_LOCAL = new ThreadLocal[SerializerInstance] {
      override protected def initialValue: SerializerInstance = {
        new KryoSerializer(conf).newInstance()
      }
    }

    def toBytes(o: Any): Array[Byte] = {
      val buf = SERIALIZER_THREAD_LOCAL.get.serialize(o)
      BinaryUtil.toBytes(buf)
    }

    def toObject(bytes: Array[Byte]): Any = {
      SERIALIZER_THREAD_LOCAL.get.deserialize(ByteBuffer.wrap(bytes))
    }
  }
}

