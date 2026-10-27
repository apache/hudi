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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.storage.row.HoodieRowCreateHandle;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link ConsistentBucketBulkInsertDataInternalWriterHelper}.
 */
class TestConsistentBucketBulkInsertDataInternalWriterHelper extends HoodieSparkClientTestHarness {

  @BeforeEach
  void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initHoodieStorage();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);
  }

  @AfterEach
  void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  void testWriteBuildsAtMostOneFileSystemView() throws Exception {
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING)
            .withBucketNum("4").build())
        .build();
    StructType structType = new StructType();
    for (String metaField : HoodieRecord.HOODIE_META_COLUMNS) {
      structType = structType.add(metaField, DataTypes.StringType);
    }
    structType = structType.add("uuid", DataTypes.StringType).add("partition", DataTypes.StringType);
    HoodieTable table = spy(HoodieSparkTable.create(config, context, metaClient));

    try (MockedConstruction<HoodieRowCreateHandle> handles = mockConstruction(HoodieRowCreateHandle.class)) {
      ConsistentBucketBulkInsertDataInternalWriterHelper helper = new ConsistentBucketBulkInsertDataInternalWriterHelper(
          table, config, "001", 0, 0L, 0L, structType, true, false);
      helper.write(new GenericInternalRow(new Object[] {
          UTF8String.fromString("001"), UTF8String.fromString("001_0_0"), UTF8String.fromString("key1"),
          UTF8String.fromString("p1"), UTF8String.fromString(""), UTF8String.fromString("key1"), UTF8String.fromString("p1")}));
      helper.close();

      assertEquals(1, handles.constructed().size());
      verify(handles.constructed().get(0)).write(any(InternalRow.class));
    }
    verify(table, atMost(1)).getFileSystemView();
  }
}
