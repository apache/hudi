/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableVersion
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestMultipleTableVersionWriting extends HoodieSparkWriterTestBase {

  @Test
  def testTableVersionAndWriteVersionMatching(): Unit = {
    val basePath = s"$tempBasePath/tbl_1"
    val df = spark.range(1).selectExpr("1 as id", "1 as name", "1 as partition")

    // write table with current version
    df.write.format("hudi")
      .option(HoodieWriteConfig.TBL_NAME.key, "tbl_1")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val metaClient = HoodieTestUtils.createMetaClient(basePath)
    assertEquals(HoodieTableVersion.current().versionCode(),
      metaClient.getTableConfig.getTableVersion.versionCode())
  }

  @Test
  def testThrowsExceptionForIncompatibleTableVersion(): Unit = {
    val basePath = s"$tempBasePath/tbl_2"
    HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.SIX)

    val df = spark.range(1).selectExpr("1 as id", "1 as name", "1 as partition")

    // should succeed when writing with auto upgrade enabled (default)
    df.write.format("hudi")
      .mode(SaveMode.Append)
      .save(basePath)

    val metaClient = HoodieTestUtils.createMetaClient(basePath)
    assertEquals(HoodieTableVersion.current().versionCode(),
      metaClient.getTableConfig.getTableVersion.versionCode())
  }
}
