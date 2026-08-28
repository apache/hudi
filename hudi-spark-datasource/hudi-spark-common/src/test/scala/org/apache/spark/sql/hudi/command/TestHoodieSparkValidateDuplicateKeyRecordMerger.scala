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

import org.apache.hudi.DefaultSparkRecordMerger

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class TestHoodieSparkValidateDuplicateKeyRecordMerger {

  @Test
  def testMergingStrategyAndPreCombiningMode(): Unit = {
    val merger = new HoodieSparkValidateDuplicateKeyRecordMerger
    // No in-repo code path instantiates this merger (see the class doc), but a table may load it through
    // HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES; the id then selects the merger and is persisted as the table's
    // merge strategy id, so pin the literal: a change to the constant would break those tables.
    assertEquals("fb092649-0fdc-4c14-9113-acde3034a6c4", merger.getMergingStrategy)
    // Pre-combining falls back to the default Spark record merger.
    assertTrue(merger.asPreCombiningMode().isInstanceOf[DefaultSparkRecordMerger])
  }
}
