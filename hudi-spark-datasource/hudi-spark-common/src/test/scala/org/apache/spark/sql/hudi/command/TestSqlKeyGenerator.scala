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
 * Tests that [[SqlKeyGenerator]] resolves a partition path without also requiring the record key.
 *
 * MOR partial updates materialise the merged record against `WRITE_PARTIAL_UPDATE_SCHEMA`, which
 * carries only the fields named in `UPDATE SET`. `HoodieIndexUtils#inferPartitionPath` then asks
 * the key generator for that record's partition path, so a record key absent from the assignments
 * is legitimately unset at that point and must not fail partition resolution.
 */
class TestSqlKeyGenerator {

  private val schema = new Schema.Parser().parse(
    s"""
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

  private def keyGenerator(partitionSchema: Option[String] = None): SqlKeyGenerator = {
    val props = new TypedProperties()
    props.put(SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME, classOf[SimpleKeyGenerator].getName)
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, "id")
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, "dt")
    // Spark SQL always sets this (see MergeIntoHoodieTableCommand), and it is what makes
    // convertPartitionPathToSqlType do any work, so cover it rather than leaving it None.
    partitionSchema.foreach(schema => props.put(SqlKeyGenerator.PARTITION_SCHEMA, schema))
    new SqlKeyGenerator(props)
  }

  /** The partition column is populated; only the record key is missing, as under a partial update. */
  private def recordWithoutRecordKey: GenericData.Record = {
    val record = new GenericData.Record(schema)
    record.put("amount", 15.0d)
    record.put("dt", "2026-08-11")
    record
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

  @Test
  def testGetPartitionPathResolvesThroughTheSqlPartitionSchema(): Unit = {
    val record = recordWithoutRecordKey
    record.put("id", 1L)
    assertEquals("2026-08-11", keyGenerator(Some("dt string")).getPartitionPath(record))
  }

  /**
   * An unresolvable partition field yields the default partition rather than an error. That is
   * KeyGenUtils#getPartitionPath substituting HUDI_DEFAULT_PARTITION_PATH for a null or absent
   * value, and it predates this change: getKey called the very same getPartitionPath, so the
   * substitution already happened whenever the record key resolved. Pinned here so the behaviour is
   * explicit, because the fix does widen when it is observable, see the sibling test below.
   */
  @Test
  def testPartitionFieldMissingFromTheSchemaYieldsTheDefaultPartition(): Unit = {
    val partialSchema = new Schema.Parser().parse(
      s"""
         |{
         |  "type": "record",
         |  "name": "test_record",
         |  "fields": [
         |    {"name": "id", "type": ["null", "long"], "default": null},
         |    {"name": "amount", "type": ["null", "double"], "default": null}
         |  ]
         |}
       """.stripMargin)
    val record = new GenericData.Record(partialSchema)
    record.put("id", 1L)
    record.put("amount", 15.0d)
    assertEquals(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH, keyGenerator().getPartitionPath(record))
  }

  /**
   * With BOTH the record key and the partition field unresolvable, the record-key exception no
   * longer pre-empts the default-partition substitution. This is the one behaviour this change
   * widens, and it is the shape a MOR partial update produces, so it is stated rather than left to
   * be discovered: resolving a partition path is not a validity check on the record.
   */
  @Test
  def testPartitionAndRecordKeyBothMissingYieldTheDefaultPartition(): Unit = {
    val partialSchema = new Schema.Parser().parse(
      s"""
         |{
         |  "type": "record",
         |  "name": "test_record",
         |  "fields": [
         |    {"name": "amount", "type": ["null", "double"], "default": null}
         |  ]
         |}
       """.stripMargin)
    val record = new GenericData.Record(partialSchema)
    record.put("amount", 15.0d)
    assertEquals(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH, keyGenerator().getPartitionPath(record))
  }
}
