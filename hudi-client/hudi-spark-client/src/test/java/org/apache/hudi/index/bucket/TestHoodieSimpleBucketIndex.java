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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestUtils.createSimpleRecord;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSimpleBucketIndex extends HoodieSparkClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieSimpleBucketIndex.class);
  private static final HoodieSchema SCHEMA = getSchemaFromResource(TestHoodieSimpleBucketIndex.class, "/exampleSchema.avsc", true);
  private static final int NUM_BUCKET = 8;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    // We have some records to be tagged (two different partitions)
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testBucketIndexValidityCheck() {
    Properties props = new Properties();
    props.setProperty(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key(), "_row_key");
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid");
    assertThrows(HoodieIndexException.class, () -> {
      HoodieIndexConfig.newBuilder().fromProperties(props)
          .withIndexType(HoodieIndex.IndexType.BUCKET)
          .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
          .withBucketNum("8").build();
    });
    props.setProperty(HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key(), "uuid");
    HoodieIndexConfig.newBuilder().fromProperties(props)
        .withIndexType(HoodieIndex.IndexType.BUCKET)
        .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
        .withBucketNum("8").build();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTagLocation(boolean isInsert) throws Exception {
    String rowKey1 = UUID.randomUUID().toString();
    String rowKey2 = UUID.randomUUID().toString();
    String rowKey3 = UUID.randomUUID().toString();
    HoodieRecord<IndexedRecord> record1 = createSimpleRecord(rowKey1, "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord<IndexedRecord> record2 = createSimpleRecord(rowKey2, "2016-01-31T03:20:41.415Z", 100);
    HoodieRecord<IndexedRecord> record3 = createSimpleRecord(rowKey3, "2016-01-31T03:16:41.415Z", 15);
    HoodieRecord<IndexedRecord> record4 = createSimpleRecord(rowKey1, "2015-01-31T03:16:41.415Z", 32);
    JavaRDD<HoodieRecord<IndexedRecord>> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record3, record4));

    HoodieWriteConfig config = makeConfig();
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    HoodieSimpleBucketIndex bucketIndex = new HoodieSimpleBucketIndex(config);
    HoodieData<HoodieRecord<IndexedRecord>> taggedRecordRDD = bucketIndex.tagLocation(HoodieJavaRDD.of(recordRDD), context, table);
    assertFalse(taggedRecordRDD.collectAsList().stream().anyMatch(r -> r.isCurrentLocationKnown()));

    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(table, SCHEMA.toAvroSchema());

    if (isInsert) {
      testTable.addCommit("001").withInserts("2016/01/31", getRecordFileId(record1), record1);
      testTable.addCommit("002").withInserts("2016/01/31", getRecordFileId(record2), record2);
      testTable.addCommit("003").withInserts("2016/01/31", getRecordFileId(record3), record3);
    } else {
      testTable.addCommit("001").withLogAppends("2016/01/31", getRecordFileId(record1), record1);
      testTable.addCommit("002").withLogAppends("2016/01/31", getRecordFileId(record2), record2);
      testTable.addCommit("003").withLogAppends("2016/01/31", getRecordFileId(record3), record3);
    }

    taggedRecordRDD = bucketIndex.tagLocation(HoodieJavaRDD.of(recordRDD), context,
        HoodieSparkTable.create(config, context, metaClient));
    assertFalse(taggedRecordRDD.collectAsList().stream().filter(r -> r.isCurrentLocationKnown())
        .filter(r -> BucketIdentifier.bucketIdFromFileId(r.getCurrentLocation().getFileId())
            != getRecordBucketId(r)).findAny().isPresent());
    assertTrue(taggedRecordRDD.collectAsList().stream().filter(r -> r.getPartitionPath().equals("2015/01/31")
            && !r.isCurrentLocationKnown()).count() == 1L);
    assertTrue(taggedRecordRDD.collectAsList().stream().filter(r -> r.getPartitionPath().equals("2016/01/31")
            && r.isCurrentLocationKnown()).count() == 3L);
  }

  private HoodieWriteConfig makeConfig() {
    Properties props = new Properties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(SCHEMA.toString())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE)
            .withIndexKeyField("_row_key")
            .withBucketNum(String.valueOf(NUM_BUCKET)).build()).build();
  }

  private String getRecordFileId(HoodieRecord record) {
    return BucketIdentifier.bucketIdStr(
        BucketIdentifier.getBucketId(record.getRecordKey(), "_row_key", NUM_BUCKET));
  }

  private int getRecordBucketId(HoodieRecord record) {
    return BucketIdentifier
        .getBucketId(record.getRecordKey(), "_row_key", NUM_BUCKET);
  }
}
