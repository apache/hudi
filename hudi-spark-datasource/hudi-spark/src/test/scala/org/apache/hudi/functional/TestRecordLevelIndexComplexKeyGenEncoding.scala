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

package org.apache.hudi.functional

import org.apache.hudi.{BloomFiltersIndexSupport, DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, RecordLevelIndexSupport}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.functional.ComplexKeyGenFixtures._
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding
import org.apache.hudi.table.upgrade.TestUpgradeDowngrade.getFixtureName

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
 * Record-key based file pruning must build its lookup key the way the table actually stores
 * `_hoodie_record_key`: a VALUE_ONLY table is looked up with the bare literal, a FIELD_PREFIXED one with
 * `field:literal`, whether the encoding is already recorded or still has to be deduced from the data.
 * Getting this wrong does not fail: it silently prunes every file and returns no rows.
 *
 * The legacy tables are the checked-in fixtures: the 1.0.2 table (bare keys) and the 0.14.0 table (prefixed keys).
 */
@Tag("functional")
class TestRecordLevelIndexComplexKeyGenEncoding extends RecordLevelIndexTestBase {

  private val probeId = "id1"
  private val probePartition = "2023-01-01"

  private def fixtureVersion(valueOnly: Boolean): HoodieTableVersion = if (valueOnly) HoodieTableVersion.EIGHT else HoodieTableVersion.SIX

  private def expectedEncoding(valueOnly: Boolean): ComplexKeyGenEncoding =
    if (valueOnly) ComplexKeyGenEncoding.VALUE_ONLY else ComplexKeyGenEncoding.FIELD_PREFIXED

  /** Extracts the fixture, points the test's base path and meta client at it, and returns its write options. */
  private def loadFixture(valueOnly: Boolean): Map[String, String] = {
    val fixtureName = getFixtureName(fixtureVersion(valueOnly), COMPLEX_KEYGEN_FIXTURE_SUFFIX)
    HoodieTestUtils.extractZipToDirectory(COMPLEX_KEYGEN_FIXTURES_PATH + fixtureName, tempDir, getClass)
    val tableName = fixtureName.replace(".zip", "")
    basePath = tempDir.resolve(tableName).toString
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    assertEquals(fixtureVersion(valueOnly), metaClient.getTableConfig.getTableVersion)
    assertFalse(metaClient.getTableConfig.getComplexKeyGenEncoding.isPresent, "Fixture tables predate the encoding property")
    fixtureWriteOpts(tableName) ++ Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
  }

  private def readOpts: Map[String, String] = Map(
    DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
    HoodieMetadataConfig.ENABLE.key -> "true")

  /** Upserts every fixture record: the upgrading write, which also builds the requested metadata indexes. */
  private def upsertFixture(opts: Map[String, String]): Unit = {
    fixtureRows(spark, FIXTURE_IDS, 10000L).write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    // the upgrade rewrote hoodie.properties (table version, timeline layout, the new property): re-read it all
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
  }

  private def candidateFiles(opts: Map[String, String], dataFilter: Expression, bloomFilters: Boolean = false): Option[Set[String]] = {
    val fileIndex = new HoodieFileIndex(spark, metaClient, Option.empty, Map("path" -> basePath), includeLogFiles = false)
    val partitionFilter: Expression = EqualTo(AttributeReference("partition", StringType)(), Literal(probePartition))
    val (_, prunedPaths) = fileIndex.prunePartitionsAndGetFileSlices(Seq.empty, Seq(partitionFilter))
    val metadataConfig = getWriteConfig(opts).getMetadataConfig
    val indexSupport = if (bloomFilters) new BloomFiltersIndexSupport(spark, metadataConfig, metaClient)
      else RecordLevelIndexSupport.create(spark, metadataConfig, metaClient)
    val candidates = indexSupport.computeCandidateFileNames(fileIndex, Seq(dataFilter), null, prunedPaths, false)
    if (candidates.isDefined) Some(candidates.get.toSet) else None
  }

  private def pointFilter: Expression = EqualTo(AttributeReference(FIXTURE_RECORD_KEY_FIELD, StringType)(), Literal(probeId))

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testPointLookupHonorsPersistedEncoding(valueOnly: Boolean): Unit = {
    val opts = loadFixture(valueOnly) + (HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true")

    // The upgraded writer, pure defaults: moves the table to the current version, records the encoding, builds the RLI.
    upsertFixture(opts)
    assertEquals(HoodieTableVersion.current(), metaClient.getTableConfig.getTableVersion)
    assertEquals(expectedEncoding(valueOnly), metaClient.getTableConfig.getComplexKeyGenEncoding.get)

    // No duplicates were introduced by the upgrade write itself, and the stored keys kept their encoding.
    val table = spark.read.format("hudi").options(readOpts).load(basePath)
    assertEquals(FIXTURE_IDS.size.toLong, table.count())
    assertEquals(table.count(), table.select("_hoodie_record_key").distinct().count(), "Upgrade write must not duplicate rows")
    val storedKey = table.filter(col(FIXTURE_RECORD_KEY_FIELD) === probeId).select("_hoodie_record_key").collect()(0).getString(0)
    assertEquals(if (valueOnly) probeId else s"$FIXTURE_RECORD_KEY_FIELD:$probeId", storedKey)

    // The recorded encoding drives the lookup literal, which must match the stored key.
    val candidates = candidateFiles(opts, pointFilter)
    assertTrue(candidates.isDefined, "Record-key pruning must be enabled")
    assertEquals(1, candidates.get.size, "The point lookup must resolve to exactly the file holding the record")

    // Query level: a silently mis-encoded lookup prunes everything and returns nothing.
    assertEquals(1L, table.filter(col(FIXTURE_RECORD_KEY_FIELD) === probeId).count())
    val inFilter: Expression = In(AttributeReference(FIXTURE_RECORD_KEY_FIELD, StringType)(), Seq(Literal(probeId), Literal("id2")))
    assertFalse(candidateFiles(opts, inFilter).exists(_.isEmpty))
    assertEquals(2L, table.filter(col(FIXTURE_RECORD_KEY_FIELD).isin(probeId, "id2")).count())

    // A table whose property went missing again: the reader deduces the encoding from the data and still prunes right.
    HoodieTableConfig.delete(metaClient.getStorage, metaClient.getMetaPath, Set(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key).asJava)
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    assertFalse(metaClient.getTableConfig.getComplexKeyGenEncoding.isPresent)
    val deducedCandidates = candidateFiles(opts, pointFilter)
    assertTrue(deducedCandidates.isDefined, "Record-key pruning must stay enabled: the encoding is deduced from the data")
    assertEquals(candidates.get, deducedCandidates.get, "The point lookup literal must match the stored key without the property too")
    assertEquals(1L, spark.read.format("hudi").options(readOpts).load(basePath).filter(col(FIXTURE_RECORD_KEY_FIELD) === probeId).count())
  }

  /** The bloom filter index shares the record key literal logic with the RLI, and must prune with the stored encoding too. */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testBloomFilterPruningHonorsRecordedEncoding(valueOnly: Boolean): Unit = {
    val opts = loadFixture(valueOnly) + (HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key -> "true")

    upsertFixture(opts)
    assertEquals(expectedEncoding(valueOnly), metaClient.getTableConfig.getComplexKeyGenEncoding.get)

    val candidates = candidateFiles(opts, pointFilter, bloomFilters = true)
    assertTrue(candidates.isDefined, "Bloom filter pruning must be enabled")
    assertEquals(1, candidates.get.size, "The bloom filter lookup must keep exactly the file holding the record")
    val table = spark.read.format("hudi").options(readOpts).load(basePath)
    assertEquals(1L, table.filter(col(FIXTURE_RECORD_KEY_FIELD) === probeId).count())
  }
}
