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

import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.exception.HoodieKeyException
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.hudi.keygen.constant.KeyGeneratorOptions

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

/**
 * Tests that [[MergeIntoKeyGenerator]] reads the meta-field ordinals only from records long enough
 * to have them.
 *
 * It resolves both the record key and the partition path by ordinal (2 and 3) off the meta-field
 * prefix, falling back to [[SqlKeyGenerator]] when the meta field is unpopulated. A MOR partial
 * update materialises the merged record against `WRITE_PARTIAL_UPDATE_SCHEMA`, so the record can be
 * shorter than the ordinal, and an unguarded read raises `ArrayIndexOutOfBoundsException` from
 * inside the key generator, naming nothing about the statement that caused it.
 */
class TestMergeIntoKeyGenerator {

  /** A record carrying only the columns an `UPDATE SET amount, ts` would assign. */
  private val partialSchema = new Schema.Parser().parse(
    """
       |{
       |  "type": "record",
       |  "name": "partial_record",
       |  "fields": [
       |    {"name": "amount", "type": ["null", "double"], "default": null},
       |    {"name": "ts", "type": ["null", "long"], "default": null}
       |  ]
       |}
     """.stripMargin)

  /** The shape the write path produces: the five meta fields, then the data columns. */
  private val metaPrefixedSchema = new Schema.Parser().parse(
    """
       |{
       |  "type": "record",
       |  "name": "meta_prefixed_record",
       |  "fields": [
       |    {"name": "_hoodie_commit_time", "type": ["null", "string"], "default": null},
       |    {"name": "_hoodie_commit_seqno", "type": ["null", "string"], "default": null},
       |    {"name": "_hoodie_record_key", "type": ["null", "string"], "default": null},
       |    {"name": "_hoodie_partition_path", "type": ["null", "string"], "default": null},
       |    {"name": "_hoodie_file_name", "type": ["null", "string"], "default": null},
       |    {"name": "id", "type": ["null", "long"], "default": null},
       |    {"name": "amount", "type": ["null", "double"], "default": null},
       |    {"name": "dt", "type": ["null", "string"], "default": null}
       |  ]
       |}
     """.stripMargin)

  private def keyGenerator: MergeIntoKeyGenerator = {
    val props = new TypedProperties()
    props.put(SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME, classOf[SimpleKeyGenerator].getName)
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "id")
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, "dt")
    props.put(SqlKeyGenerator.PARTITION_SCHEMA, "dt string")
    new MergeIntoKeyGenerator(props)
  }

  private def partialRecord: GenericData.Record = {
    val record = new GenericData.Record(partialSchema)
    record.put("amount", 15.0d)
    record.put("ts", 200L)
    record
  }

  /**
   * Two fields against partition-path ordinal 3. Before the guard this raised
   * ArrayIndexOutOfBoundsException; it now falls through to the SQL key generator, which resolves
   * the absent partition field to the default partition.
   */
  @Test
  def testGetPartitionPathFallsBackOnARecordShorterThanTheOrdinal(): Unit = {
    assertEquals(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH,
      keyGenerator.getPartitionPath(partialRecord))
  }

  /**
   * Same record against record-key ordinal 2. The fallback reaches record-key validation, so the
   * failure names the field rather than an array bound.
   */
  @Test
  def testGetRecordKeyFallsBackOnARecordShorterThanTheOrdinal(): Unit = {
    val thrown = assertThrows(classOf[HoodieKeyException], () => keyGenerator.getRecordKey(partialRecord))
    assert(thrown.getMessage.contains("id"), s"expected the message to name the field, got: ${thrown.getMessage}")
  }

  /**
   * Regression guard for the normal path: a meta-prefixed record is long enough, so both accessors
   * still read the ordinal rather than falling back. Without this the guard could disable meta-field
   * resolution outright and the tests above would still pass.
   */
  @Test
  def testMetaPrefixedRecordStillResolvesFromTheMetaFields(): Unit = {
    val record = new GenericData.Record(metaPrefixedSchema)
    record.put("_hoodie_record_key", "id:1")
    record.put("_hoodie_partition_path", "dt=2026-08-11")
    record.put("id", 1L)
    record.put("dt", "2026-08-12") // deliberately disagrees, to show the meta field is what is read
    assertEquals("id:1", keyGenerator.getRecordKey(record))
    assertEquals("dt=2026-08-11", keyGenerator.getPartitionPath(record))
  }

  /**
   * A meta-prefixed record whose meta fields are unpopulated is still long enough, so the ordinal is
   * read, found null, and the existing fallback runs. Pins that the guard did not change which of
   * the two fallback reasons applies.
   */
  @Test
  def testMetaPrefixedRecordWithNullMetaFieldsFallsBackToTheDataColumns(): Unit = {
    val record = new GenericData.Record(metaPrefixedSchema)
    record.put("id", 1L)
    record.put("dt", "2026-08-11")
    assertEquals("1", keyGenerator.getRecordKey(record))
    assertEquals("2026-08-11", keyGenerator.getPartitionPath(record))
  }
}
