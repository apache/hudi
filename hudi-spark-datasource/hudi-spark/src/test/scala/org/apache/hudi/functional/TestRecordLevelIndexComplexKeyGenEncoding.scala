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

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, RecordLevelIndexSupport}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, In, Literal}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.JavaConverters._

/**
 * The record level index point lookup must build its lookup key the way the table actually stores
 * `_hoodie_record_key`. For a single-field ComplexKeyGenerator that is unknown below table version 9
 * (pruning is disabled there) and, from version 9 on, given by the encoding the 8 -> 9 upgrade persisted:
 * a VALUE_ONLY table is looked up with the bare literal, a FIELD_PREFIXED one with `field:literal`.
 * Getting this wrong does not fail: it silently prunes every file and returns no rows.
 */
@Tag("functional")
class TestRecordLevelIndexComplexKeyGenEncoding extends RecordLevelIndexTestBase {

  private val recordKeyField = "_row_key"

  private def legacyOpts(valueOnly: Boolean): Map[String, String] = commonOpts ++ metadataOpts ++ Map(
    DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyField,
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.ComplexKeyGenerator",
    DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
    HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8",
    HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key ->
      (if (valueOnly) ComplexKeyGenEncoding.VALUE_ONLY else ComplexKeyGenEncoding.FIELD_PREFIXED).name)

  private def upgradedOpts(valueOnly: Boolean): Map[String, String] = legacyOpts(valueOnly) --
    Seq(HoodieWriteConfig.WRITE_TABLE_VERSION.key, HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key)

  private def readOpts: Map[String, String] = Map(
    DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
    HoodieMetadataConfig.ENABLE.key -> "true")

  private def candidateFiles(opts: Map[String, String], selectedPartition: String, dataFilter: Expression): Option[Set[String]] = {
    val fileIndex = new HoodieFileIndex(spark, metaClient, Option.empty, Map("path" -> basePath), includeLogFiles = false)
    val partitionFilter: Expression = EqualTo(AttributeReference("partition", StringType)(), Literal(selectedPartition))
    val (_, prunedPaths) = fileIndex.prunePartitionsAndGetFileSlices(Seq.empty, Seq(partitionFilter))
    val rliIndexSupport = RecordLevelIndexSupport.create(spark, getWriteConfig(opts).getMetadataConfig, metaClient)
    val candidates = rliIndexSupport.computeCandidateFileNames(fileIndex, Seq(dataFilter), null, prunedPaths, false)
    if (candidates.isDefined) Some(candidates.get.toSet) else None
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPointLookupHonorsPersistedEncoding(valueOnly: Boolean): Unit = {
    // A version 8 table written the legacy way (bare keys when valueOnly, field-prefixed otherwise), with the RLI on.
    val df = doWriteAndValidateDataAndRecordIndex(legacyOpts(valueOnly),
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    val probe = df.limit(1).collect()(0)
    val selectedPartition = probe.getAs[String]("partition")
    val recordKey = probe.getAs[String](recordKeyField)
    val dataFilter: Expression = EqualTo(AttributeReference(recordKeyField, StringType)(), Literal(recordKey))

    // Simulate a table written before the encoding was recorded: the reader then deduces it from the data.
    metaClient = getLatestMetaClient(true)
    HoodieTableConfig.delete(metaClient.getStorage, metaClient.getMetaPath, Set(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key).asJava)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertFalse(metaClient.getTableConfig.getComplexKeyGenEncoding.isPresent)
    val legacyCandidates = candidateFiles(legacyOpts(valueOnly), selectedPartition, dataFilter)
    assertTrue(legacyCandidates.isDefined, "Record-key pruning must stay enabled: the encoding is deduced from the data")
    assertEquals(1, legacyCandidates.get.size, "The point lookup literal must match the stored key even before the backfill")

    // The upgraded writer, pure defaults: moves the table to the current version and persists the encoding.
    doWriteAndValidateDataAndRecordIndex(upgradedOpts(valueOnly),
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)
    // getLatestMetaClient only reloads the timeline; the upgrade rewrote hoodie.properties, so the table
    // config has to be re-read or the version and the new encoding property are both read stale.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val expectedEncoding = if (valueOnly) ComplexKeyGenEncoding.VALUE_ONLY else ComplexKeyGenEncoding.FIELD_PREFIXED
    assertEquals(HoodieTableVersion.current(), metaClient.getTableConfig.getTableVersion)
    assertEquals(expectedEncoding, metaClient.getTableConfig.getComplexKeyGenEncoding.get)

    // No duplicates were introduced by the upgrade write itself.
    val table = spark.read.format("hudi").options(readOpts).load(basePath)
    assertEquals(table.count(), table.select("_hoodie_record_key").distinct().count(), "Upgrade write must not duplicate rows")
    val storedKey = table.filter(col(recordKeyField) === recordKey).select("_hoodie_record_key").collect()(0).getString(0)
    assertEquals(if (valueOnly) recordKey else s"$recordKeyField:$recordKey", storedKey)

    // The recorded encoding drives the lookup literal, which must match the stored key.
    val candidates = candidateFiles(upgradedOpts(valueOnly), selectedPartition, dataFilter)
    assertTrue(candidates.isDefined, "Record-key pruning must be enabled")
    assertEquals(1, candidates.get.size, "The point lookup must resolve to exactly the file holding the record")

    // Query level: a silently mis-encoded lookup prunes everything and returns nothing.
    assertEquals(1L, table.filter(col(recordKeyField) === recordKey).count())
    val secondKey = df.filter(col(recordKeyField) =!= recordKey).limit(1).collect()(0).getAs[String](recordKeyField)
    val inFilter: Expression = In(AttributeReference(recordKeyField, StringType)(), Seq(Literal(recordKey), Literal(secondKey)))
    assertFalse(candidateFiles(upgradedOpts(valueOnly), selectedPartition, inFilter).exists(_.isEmpty))
    assertEquals(2L, table.filter(col(recordKeyField).isin(recordKey, secondKey)).count())
  }
}
