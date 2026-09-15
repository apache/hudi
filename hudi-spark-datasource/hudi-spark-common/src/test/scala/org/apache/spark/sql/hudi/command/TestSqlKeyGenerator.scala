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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.common.config.TimestampKeyGeneratorConfig
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.exception.HoodieKeyException
import org.apache.hudi.keygen.{KeyGenUtils, SimpleKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.joda.time.DateTimeZone
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._

/**
 * Tests that [[SqlKeyGenerator]] resolves a partition path without also requiring the record key.
 *
 * MOR partial updates materialise the merged record against `WRITE_PARTIAL_UPDATE_SCHEMA`, which
 * carries only the fields named in `UPDATE SET`. `HoodieIndexUtils#inferPartitionPath` then asks
 * the key generator for that record's partition path, so a record key absent from the assignments
 * is legitimately unset at that point and must not fail partition resolution.
 *
 * The fixtures set `hoodie.sql.partition.schema` by default because production always does, at
 * every construction site (`ProvidesHoodieConfig` and both `MergeIntoHoodieTableCommand` copies).
 * Leaving it unset makes `convertPartitionPathToSqlType` return its input immediately, which skips
 * the default-partition guard, the fragment-count early-out and hive-style handling, so the cases
 * named after those behaviours would never reach them.
 */
class TestSqlKeyGenerator {

  private val schema = new Schema.Parser().parse(
    """
       |{
       |  "type": "record",
       |  "name": "test_record",
       |  "fields": [
       |    {"name": "id", "type": ["null", "long"], "default": null},
       |    {"name": "amount", "type": ["null", "double"], "default": null},
       |    {"name": "dt", "type": ["null", "string"], "default": null}
       |  ]
       |}
     """.stripMargin)

  /** 2026-08-11 00:00:00 UTC in microseconds, which is what the GenericRecord path assumes. */
  private val timestampMicros = String.valueOf(1786406400000000L)

  /** The same record shape with only the named fields, as a partial update produces. */
  private def projected(fieldNames: String*): Schema = {
    val fields = schema.getFields.asScala
      .filter(f => fieldNames.contains(f.name))
      .map(f => new Schema.Field(f.name, f.schema, null, f.defaultVal))
    Schema.createRecord("test_record", null, null, false, fields.asJava)
  }

  private def keyGenerator(partitionSchema: Option[String] = Some("dt string"),
                           withRecordKey: Boolean = true): SqlKeyGenerator = {
    val props = new TypedProperties()
    props.put(SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME, classOf[SimpleKeyGenerator].getName)
    if (withRecordKey) {
      props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "id")
    }
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, "dt")
    // Production sets both whenever record keys are auto-generated (HoodieCreateRecordUtils), so
    // without them the auto-record-key delegate fails on the missing property rather than on the
    // behaviour under test.
    props.put(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG, "100")
    props.put(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG, "1")
    partitionSchema.foreach(ps => props.put(SqlKeyGenerator.PARTITION_SCHEMA, ps))
    new SqlKeyGenerator(props)
  }

  /** Delegates to a TimestampBasedKeyGenerator rather than a SimpleKeyGenerator. */
  private def timestampKeyGenerator(partitionSchema: Option[String]): SqlKeyGenerator = {
    val props = new TypedProperties()
    props.put(SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME, classOf[TimestampBasedKeyGenerator].getName)
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "id")
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, "dt")
    props.put(TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD.key, "DATE_STRING")
    props.put(TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT.key, "yyyy-MM-dd")
    props.put(TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy-MM-dd")
    partitionSchema.foreach(ps => props.put(SqlKeyGenerator.PARTITION_SCHEMA, ps))
    new SqlKeyGenerator(props)
  }

  /** The partition column is populated; only the record key is missing, as under a partial update. */
  private def recordWithoutRecordKey: GenericData.Record = {
    val record = new GenericData.Record(schema)
    record.put("amount", 15.0d)
    record.put("dt", "2026-08-11")
    record
  }

  /** A partial-update shape: the partition field is not in the schema at all. */
  private def recordMissingThePartitionField: GenericData.Record = {
    val record = new GenericData.Record(projected("id", "amount"))
    record.put("id", 1L)
    record.put("amount", 15.0d)
    record
  }

  /** Runs `f` with the default timezone pinned, since the timestamp arms format in it. */
  private def inUtc[T](f: => T): T = {
    val previousZone = DateTimeZone.getDefault
    DateTimeZone.setDefault(DateTimeZone.UTC)
    try f finally DateTimeZone.setDefault(previousZone)
  }

  @Test
  def testGetPartitionPathDoesNotRequireRecordKey(): Unit = {
    // Before the fix this threw HoodieKeyException, because getPartitionPath delegated to
    // BaseKeyGenerator#getKey, which builds the whole HoodieKey and so validates the record key.
    assertEquals("2026-08-11", keyGenerator().getPartitionPath(recordWithoutRecordKey))
  }

  @Test
  def testGetRecordKeyStillRejectsAMissingRecordKey(): Unit = {
    // Scope guard: the fix must not weaken record-key validation, only stop getPartitionPath from
    // triggering it. A record key that is genuinely required and absent is still an error.
    assertThrows(classOf[HoodieKeyException], () => keyGenerator().getRecordKey(recordWithoutRecordKey))
  }

  @Test
  def testGetPartitionPathAndRecordKeyOnACompleteRecord(): Unit = {
    val record = recordWithoutRecordKey
    record.put("id", 1L)
    assertEquals("2026-08-11", keyGenerator().getPartitionPath(record))
    assertEquals("1", keyGenerator().getRecordKey(record))
  }

  /**
   * Drives convertPartitionPathToSqlType's TimestampType arm, the only arm that rewrites the value.
   * The expected string was captured from a run rather than derived.
   */
  @Test
  def testGetPartitionPathConvertsATimestampPartitionValue(): Unit = inUtc {
    val record = new GenericData.Record(schema)
    record.put("id", 1L)
    record.put("dt", timestampMicros)
    assertEquals("2026-08-11 00%3A00%3A00",
      keyGenerator(Some("dt timestamp")).getPartitionPath(record))
  }

  /**
   * The one shape that legitimately supplies no partition schema is a non-partitioned table, where
   * convertPartitionPathToSqlType returns its input untouched. Paired with the case above on the
   * same value, so the conversion is shown to be skipped rather than merely absent.
   */
  @Test
  def testNonPartitionedTableLeavesThePartitionValueUnconverted(): Unit = inUtc {
    val record = new GenericData.Record(schema)
    record.put("id", 1L)
    record.put("dt", timestampMicros)
    assertEquals(timestampMicros, keyGenerator(partitionSchema = None).getPartitionPath(record))
  }

  /**
   * A TimestampBasedKeyGenerator delegate does NOT substitute HUDI_DEFAULT_PARTITION_PATH for an
   * absent partition field: it formats the epoch instead, so the HUDI-8315 guard in
   * convertPartitionPathToSqlType never fires for it. Pinned because this change makes the case
   * reachable, where previously the record-key exception pre-empted it.
   *
   * All three partition-schema spellings are pinned separately below because they diverge, and the
   * divergence is the point: a `timestamp` column turns the epoch string into a bare
   * NumberFormatException, while a `string` column silently accepts the epoch partition.
   */
  @Test
  def testTimestampDelegateResolvesAnAbsentPartitionFieldToTheEpoch(): Unit = inUtc {
    assertEquals("1970-01-01",
      timestampKeyGenerator(None).getPartitionPath(recordMissingThePartitionField))
  }

  @Test
  def testTimestampDelegateSilentlyAcceptsTheEpochUnderAStringPartitionSchema(): Unit = inUtc {
    assertEquals("1970-01-01",
      timestampKeyGenerator(Some("dt string")).getPartitionPath(recordMissingThePartitionField))
  }

  @Test
  def testTimestampDelegateThrowsUnderATimestampPartitionSchema(): Unit = inUtc {
    // The epoch string the delegate produced is not microseconds, so the TimestampType arm cannot
    // parse it. NumberFormatException rather than a Hudi exception is what the code does today.
    assertThrows(classOf[NumberFormatException],
      () => timestampKeyGenerator(Some("dt timestamp")).getPartitionPath(recordMissingThePartitionField))
  }

  /**
   * Without a record-key config KeyGenUtils#isAutoGeneratedRecordKeysEnabled is true, so the delegate
   * is wrapped in an AutoRecordGenWrapperKeyGenerator. That wrapper is a BaseKeyGenerator, so it now
   * takes the direct arm and getPartitionPath no longer consumes a generated sequence id as a side
   * effect of building a HoodieKey.
   *
   * The record key is asserted after two partition lookups to pin that stride: the sequence id is
   * `instantTime_partitionId_rowId`, so it reads 100_1_0 here and 100_1_2 before the change.
   * Uniqueness never depended on the stride, but it is now a pinned decision.
   */
  @Test
  def testAutoRecordKeyDelegateDoesNotConsumeSequenceIdsOnPartitionLookups(): Unit = {
    val record = new GenericData.Record(schema)
    record.put("amount", 15.0d)
    record.put("dt", "2026-08-11")
    val keyGen = keyGenerator(withRecordKey = false)
    assertEquals("2026-08-11", keyGen.getPartitionPath(record))
    assertEquals("2026-08-11", keyGen.getPartitionPath(record))
    assertEquals("100_1_0", keyGen.getRecordKey(record))
  }

  /**
   * An unresolvable partition field yields the default partition rather than an error. That is
   * KeyGenUtils#getPartitionPath substituting HUDI_DEFAULT_PARTITION_PATH for a null or absent
   * value, and it predates this change: getKey called the very same getPartitionPath, so the
   * substitution already happened whenever the record key resolved. Pinned here so the behaviour is
   * explicit, because the fix does widen when it is observable, see the sibling test below.
   *
   * NOTE this is the behaviour TestSimpleKeyGenerator marks with "TODO this should throw as well" on
   * the Avro path, its Row-path twin already asserting HoodieException. If that TODO is addressed,
   * this expectation changes with it; the sibling test below is the one that discriminates the fix.
   */
  @Test
  def testPartitionFieldMissingFromTheSchemaYieldsTheDefaultPartition(): Unit = {
    assertEquals(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH,
      keyGenerator().getPartitionPath(recordMissingThePartitionField))
  }

  /**
   * With BOTH the record key and the partition field unresolvable, the record-key exception no
   * longer pre-empts the default-partition substitution. This is the one behaviour this change
   * widens, and it is the shape a MOR partial update produces, so it is stated rather than left to
   * be discovered: resolving a partition path is not a validity check on the record.
   */
  @Test
  def testPartitionAndRecordKeyBothMissingYieldTheDefaultPartition(): Unit = {
    val record = new GenericData.Record(projected("amount"))
    record.put("amount", 15.0d)
    assertEquals(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH,
      keyGenerator().getPartitionPath(record))
  }
}
