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

import org.apache.hudi.SparkFileFormatInternalRowReaderContext.{filtersAreSafeForBootstrap, filtersAreSafeForMor}
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.sources.{And, IsNotNull, IsNull, Or}
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.{Tag, Test}

@Tag("functional")
class TestFileGroupReaderFilterUtils {

  @Test
  def testMorFilters(): Unit = {
    val recordKeyField = HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName
    assertTrue(filtersAreSafeForMor(Seq.empty, recordKeyField))

    val recordKeyFilter = IsNotNull(recordKeyField)
    assertTrue(filtersAreSafeForMor(Seq(recordKeyFilter), recordKeyField))
    assertTrue(filtersAreSafeForMor(Seq(recordKeyFilter, recordKeyFilter, recordKeyFilter), recordKeyField))
    val differentRecordKeyFilter = IsNull(recordKeyField)
    assertTrue(filtersAreSafeForMor(Seq(recordKeyFilter, differentRecordKeyFilter), recordKeyField))

    val otherFieldFilter = IsNotNull("someotherfield")
    assertFalse(filtersAreSafeForMor(Seq(otherFieldFilter), recordKeyField))
    assertFalse(filtersAreSafeForMor(Seq(otherFieldFilter, recordKeyFilter), recordKeyField))

    val complexFilter = Or(recordKeyFilter, otherFieldFilter)
    assertFalse(filtersAreSafeForMor(Seq(complexFilter), recordKeyField))

    val legalComplexFilter = Or(recordKeyFilter, differentRecordKeyFilter)
    assertTrue(filtersAreSafeForMor(Seq(legalComplexFilter),recordKeyField))

    val nestedFilter = And(complexFilter, legalComplexFilter)
    assertFalse(filtersAreSafeForMor(Seq(nestedFilter), recordKeyField))

    val legalNestedFilter = And(legalComplexFilter, recordKeyFilter)
    assertTrue(filtersAreSafeForMor(Seq(legalNestedFilter), recordKeyField))
  }

  @Test
  def testBootstrapFilters(): Unit = {
    val recordKeyField = HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName
    val commitTimeField = HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD.getFieldName
    assertTrue(filtersAreSafeForBootstrap(Seq.empty))

    val recordKeyFilter = IsNotNull(recordKeyField)
    assertTrue(filtersAreSafeForBootstrap(Seq(recordKeyFilter)))
    assertTrue(filtersAreSafeForBootstrap(Seq(recordKeyFilter, recordKeyFilter, recordKeyFilter)))
    val commitTimeFilter = IsNotNull(commitTimeField)
    assertTrue(filtersAreSafeForBootstrap(Seq(recordKeyFilter, commitTimeFilter)))

    val dataFieldFilter = IsNotNull("someotherfield")
    assertTrue(filtersAreSafeForBootstrap(Seq(dataFieldFilter)))
    assertTrue(filtersAreSafeForBootstrap(Seq(dataFieldFilter, recordKeyFilter)))

    val legalComplexFilter = Or(recordKeyFilter, commitTimeFilter)
    assertTrue(filtersAreSafeForBootstrap(Seq(legalComplexFilter)))
    assertTrue(filtersAreSafeForBootstrap(Seq(dataFieldFilter, dataFieldFilter)))

    val illegalComplexFilter = Or(recordKeyFilter, dataFieldFilter)
    assertFalse(filtersAreSafeForBootstrap(Seq(illegalComplexFilter)))
    assertFalse(filtersAreSafeForBootstrap(Seq(illegalComplexFilter, dataFieldFilter, recordKeyFilter)))

    val illegalNestedFilter = And(legalComplexFilter, illegalComplexFilter)
    assertFalse(filtersAreSafeForBootstrap(Seq(illegalNestedFilter)))

    val legalNestedFilter = And(legalComplexFilter, recordKeyFilter)
    assertTrue(filtersAreSafeForBootstrap(Seq(legalNestedFilter)))
  }

}
