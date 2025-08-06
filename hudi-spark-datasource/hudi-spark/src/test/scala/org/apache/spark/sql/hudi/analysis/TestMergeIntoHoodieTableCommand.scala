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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.index.HoodieIndex

import org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestMergeIntoHoodieTableCommand extends AnyFlatSpec with Matchers {
  "useGlobalIndex" should "return true when global index is enabled" in {
    val globalIndices = Seq(
      HoodieIndex.IndexType.GLOBAL_SIMPLE -> HoodieIndexConfig.SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE,
      HoodieIndex.IndexType.GLOBAL_BLOOM -> HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE,
      HoodieIndex.IndexType.RECORD_INDEX -> HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE
    )

    globalIndices.foreach { case (indexType, config) =>
      val parameters = Map(
        HoodieIndexConfig.INDEX_TYPE.key -> indexType.name,
        config.key -> "true"
      )
      MergeIntoHoodieTableCommand.useGlobalIndexAndUpdatePartitionPath(parameters) should be(true)
    }
  }

  it should "return false when update partition path is disabled" in {
    val globalIndices = Seq(
      HoodieIndex.IndexType.GLOBAL_SIMPLE -> HoodieIndexConfig.SIMPLE_INDEX_UPDATE_PARTITION_PATH_ENABLE,
      HoodieIndex.IndexType.GLOBAL_BLOOM -> HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH_ENABLE,
      HoodieIndex.IndexType.RECORD_INDEX -> HoodieIndexConfig.RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE
    )

    globalIndices.foreach { case (indexType, config) =>
      val parameters = Map(
        HoodieIndexConfig.INDEX_TYPE.key -> indexType.name,
        config.key -> "false"
      )
      MergeIntoHoodieTableCommand.useGlobalIndexAndUpdatePartitionPath(parameters) should be(false)
    }
  }

  it should "return false for record index without setting update partition path (default false)" in {
    val parameters = Map(
      HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.RECORD_INDEX.name
    )
    MergeIntoHoodieTableCommand.useGlobalIndexAndUpdatePartitionPath(parameters) should be(false)
  }

  it should "return false when index type is absent" in {
    val parameters = Map.empty[String, String]
    MergeIntoHoodieTableCommand.useGlobalIndexAndUpdatePartitionPath(parameters) should be(false)
  }

  it should "return false when index type is not global" in {
    val nonGlobalIndices = Seq(
      HoodieIndex.IndexType.SIMPLE.name,
      HoodieIndex.IndexType.INMEMORY.name,
      HoodieIndex.IndexType.BLOOM.name,
      HoodieIndex.IndexType.BUCKET.name,
      HoodieIndex.IndexType.FLINK_STATE.name)
    nonGlobalIndices.foreach { indexType =>
      val parameters = Map(HoodieIndexConfig.INDEX_TYPE.key -> indexType)
      MergeIntoHoodieTableCommand.useGlobalIndexAndUpdatePartitionPath(parameters) should be(false)
    }
  }

  it should "return true for CUSTOM merge mode" in {
    val params = Map(
      DataSourceWriteOptions.RECORD_MERGE_MODE.key -> RecordMergeMode.CUSTOM.name
    )
    MergeIntoHoodieTableCommand.useCustomMergeMode(params) should be(true)
  }

  it should "return false for non-CUSTOM merge mode" in {
    var params = Map(
      DataSourceWriteOptions.RECORD_MERGE_MODE.key -> RecordMergeMode.COMMIT_TIME_ORDERING.name
    )
    MergeIntoHoodieTableCommand.useCustomMergeMode(params) should be(false)

    params = Map(
      DataSourceWriteOptions.RECORD_MERGE_MODE.key -> RecordMergeMode.EVENT_TIME_ORDERING.name
    )
    MergeIntoHoodieTableCommand.useCustomMergeMode(params) should be(false)
  }

  it should "throw HoodieException when merge mode is missing" in {
    val params = Map.empty[String, String]
    an[HoodieException] should be thrownBy MergeIntoHoodieTableCommand.useCustomMergeMode(params)
  }
}
