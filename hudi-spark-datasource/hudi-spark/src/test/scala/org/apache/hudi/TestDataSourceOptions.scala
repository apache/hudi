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

package org.apache.hudi

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.hive.{HiveStylePartitionValueExtractor, MultiPartKeysValueExtractor}
import org.apache.hudi.keygen.{ComplexKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestDataSourceOptions {
  @Test def inferDataSourceOptions(): Unit = {
    val inputOptions1 = Map(
      RECORDKEY_FIELD.key -> "uuid",
      TABLE_NAME.key -> "hudi_table",
      PARTITIONPATH_FIELD.key -> "year,month"
    )
    val modifiedOptions1 = HoodieWriterUtils.parametersWithWriteDefaults(inputOptions1)
    assertEquals(classOf[ComplexKeyGenerator].getName, modifiedOptions1(KEYGENERATOR_CLASS_NAME.key))
    assertEquals("hudi_table", modifiedOptions1(HoodieSyncConfig.META_SYNC_TABLE_NAME.key))
    assertEquals("year,month", modifiedOptions1(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key))
    assertEquals(classOf[MultiPartKeysValueExtractor].getName,
      modifiedOptions1(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key))

    val inputOptions2 = Map(
      RECORDKEY_FIELD.key -> "uuid",
      TABLE_NAME.key -> "hudi_table",
      PARTITIONPATH_FIELD.key -> "year",
      HIVE_STYLE_PARTITIONING.key -> "true"
    )
    val modifiedOptions2 = HoodieWriterUtils.parametersWithWriteDefaults(inputOptions2)
    assertEquals(classOf[SimpleKeyGenerator].getName, modifiedOptions2(KEYGENERATOR_CLASS_NAME.key))
    assertEquals("hudi_table", modifiedOptions2(HoodieSyncConfig.META_SYNC_TABLE_NAME.key))
    assertEquals("year", modifiedOptions2(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key))
    assertEquals(classOf[HiveStylePartitionValueExtractor].getName,
      modifiedOptions2(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key))
  }
}
