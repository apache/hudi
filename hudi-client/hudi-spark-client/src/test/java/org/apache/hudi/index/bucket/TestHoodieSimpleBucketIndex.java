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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.AbstractTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.ExecutorTimelineListingCountingFileSystem;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestUtils.createSimpleRecord;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.testutils.TaskPayloadTestUtils.assertNoHeavyTypesInLineage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TestHoodieSimpleBucketIndex extends HoodieSparkClientTestHarness {
  private static final HoodieSchema SCHEMA = getSchemaFromResource(TestHoodieSimpleBucketIndex.class, "/exampleSchema.avsc", true);
  private static final int NUM_BUCKET = 8;
  private static final List<String> INDEX_KEY_FIELDS = Collections.singletonList("_row_key");

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

    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(table, SCHEMA);

    if (isInsert) {
      testTable.addCommit("001").withInserts("2016/01/31", getRecordFileId(record1), record1);
      testTable.addCommit("002").withInserts("2016/01/31", getRecordFileId(record2), record2);
      testTable.addCommit("003").withInserts("2016/01/31", getRecordFileId(record3), record3);
    } else {
      testTable.addCommit("001").withLogAppends("2016/01/31", getRecordFileId(record1), record1);
      testTable.addCommit("002").withLogAppends("2016/01/31", getRecordFileId(record2), record2);
      testTable.addCommit("003").withLogAppends("2016/01/31", getRecordFileId(record3), record3);
    }

    metaClient.reloadActiveTimeline();
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

  @Test
  void testTagLocationTaskCarriesNoTable() {
    HoodieWriteConfig config = makeConfig();
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    JavaRDD<HoodieRecord<IndexedRecord>> recordRDD = jsc.parallelize(Arrays.asList(
        createSimpleRecord(UUID.randomUUID().toString(), "2016-01-31T03:16:41.415Z", 12)));

    HoodieData<HoodieRecord<IndexedRecord>> taggedRecords =
        new HoodieSimpleBucketIndex(config).tagLocation(HoodieJavaRDD.of(recordRDD), context, table);
    assertNoHeavyTypesInLineage(HoodieJavaRDD.getJavaRDD(taggedRecords).rdd());
  }

  @Test
  void testTagLocationDoesNotListTimelineOnExecutors() throws Exception {
    HoodieRecord<IndexedRecord> record1 = createSimpleRecord(UUID.randomUUID().toString(), "2016-01-31T03:16:41.415Z", 12);
    HoodieRecord<IndexedRecord> record2 = createSimpleRecord(UUID.randomUUID().toString(), "2015-01-31T03:16:41.415Z", 32);
    HoodieWriteConfig config = makeConfig();
    HoodieSparkWriteableTestTable.of(HoodieSparkTable.create(config, context, metaClient), SCHEMA)
        .addCommit("001").withInserts("2016/01/31", getRecordFileId(record1), record1);

    HoodieTableMetaClient countingMetaClient = HoodieTableMetaClient.builder()
        .setConf(ExecutorTimelineListingCountingFileSystem.withCountingFileSystem(storageConf))
        .setBasePath(basePath)
        .build();
    HoodieTable table = HoodieSparkTable.create(config, context, countingMetaClient);
    JavaRDD<HoodieRecord<IndexedRecord>> recordRDD = jsc.parallelize(Arrays.asList(record1, record2, record1), 3);

    ExecutorTimelineListingCountingFileSystem.reset();
    List<HoodieRecord<IndexedRecord>> taggedRecords =
        new HoodieSimpleBucketIndex(config).tagLocation(HoodieJavaRDD.of(recordRDD), context, table).collectAsList();

    assertEquals(0, ExecutorTimelineListingCountingFileSystem.executorTimelineListings());
    assertEquals(3, taggedRecords.size());
    taggedRecords.forEach(record -> {
      if (record.getPartitionPath().equals("2016/01/31")) {
        assertEquals("001", record.getCurrentLocation().getInstantTime());
        assertTrue(record.getCurrentLocation().getFileId().startsWith(getRecordFileId(record1)));
      } else {
        assertFalse(record.isCurrentLocationKnown());
      }
    });
  }

  /**
   * A Spark executor gets a deserialized loader without the table and builds its own view from the
   * writer's timeline.
   */
  @Test
  void testDeserializedLocationLoaderInExecutorTask() throws Exception {
    HoodieRecord<IndexedRecord> record = createSimpleRecord(UUID.randomUUID().toString(), "2016-01-31T03:16:41.415Z", 12);
    HoodieWriteConfig config = makeConfig();
    HoodieSparkWriteableTestTable.of(HoodieSparkTable.create(config, context, metaClient), SCHEMA)
        .addCommit("001").withInserts("2016/01/31", getRecordFileId(record), record);
    HoodieTableMetaClient countingMetaClient = HoodieTableMetaClient.builder()
        .setConf(ExecutorTimelineListingCountingFileSystem.withCountingFileSystem(storageConf))
        .setBasePath(basePath)
        .build();
    byte[] loaderBytes = javaSerialize(
        new HoodieSimpleBucketIndex.BucketLocationLoader(HoodieSparkTable.create(config, context, countingMetaClient), INDEX_KEY_FIELDS));
    int bucketId = getRecordBucketId(record);

    ExecutorTimelineListingCountingFileSystem.reset();
    List<String> locations = jsc.parallelize(Arrays.asList("2016/01/31", "2015/01/31"), 2)
        .map(partition -> {
          HoodieSimpleBucketIndex.BucketLocationLoader loader = javaDeserialize(loaderBytes);
          HoodieRecordLocation location = loader.getBucketIdToLocation(partition).get(bucketId);
          return partition + "=" + (location == null ? "none" : location.getInstantTime() + "/" + location.getFileId());
        })
        .collect();

    assertEquals(0, ExecutorTimelineListingCountingFileSystem.executorTimelineListings());
    assertEquals(Arrays.asList("2016/01/31=001/" + getRecordFileId(record), "2015/01/31=none"), locations);
  }

  /**
   * A worker reads the metadata table like HoodieTable does: with a reader that opens the metadata files per
   * lookup and closes them after, not the reusable reader the timeline server keeps open.
   */
  @Test
  void testWorkerViewReadsMetadataTableWithoutReusableReader() throws Exception {
    HoodieRecord<IndexedRecord> record = createSimpleRecord(UUID.randomUUID().toString(), "2016-01-31T03:16:41.415Z", 12);
    HoodieWriteConfig config = makeConfig();
    String fileId = getRecordFileId(record);
    try (HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(storageConf, config, context)) {
      HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(metaClient, SCHEMA, metadataWriter, Option.of(context));
      StoragePath baseFile = testTable.forCommit("001").withInserts("2016/01/31", fileId, Collections.singletonList(record));
      Map<String, List<Pair<String, Integer>>> partitionToFiles = Collections.singletonMap("2016/01/31",
          Collections.singletonList(Pair.of(fileId, (int) storage.getPathInfo(baseFile).getLength())));
      testTable.doWriteOperation("001", WriteOperationType.UPSERT, Collections.singletonList("2016/01/31"),
          partitionToFiles, false, false);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getTableConfig().isMetadataTableAvailable());

    HoodieSimpleBucketIndex.BucketLocationLoader loader = javaDeserialize(javaSerialize(
        new HoodieSimpleBucketIndex.BucketLocationLoader(HoodieSparkTable.create(config, context, metaClient), INDEX_KEY_FIELDS)));
    HoodieRecordLocation location = loader.getBucketIdToLocation("2016/01/31").get(getRecordBucketId(record));

    assertEquals("001", location.getInstantTime());
    assertTrue(location.getFileId().startsWith(fileId));
    HoodieTableMetadata tableMetadata = (HoodieTableMetadata) readField(loader.getWorkerView(), AbstractTableFileSystemView.class, "tableMetadata");
    assertInstanceOf(HoodieBackedTableMetadata.class, tableMetadata);
    assertFalse((boolean) readField(tableMetadata, HoodieBackedTableMetadata.class, "reuse"));
  }

  @Test
  void testSubclassHooksStillApply() {
    HoodieWriteConfig config = makeConfig();
    HoodieRecordLocation location = new HoodieRecordLocation("001", "00000000-fixed-file");
    JavaRDD<HoodieRecord<IndexedRecord>> recordRDD = jsc.parallelize(Arrays.asList(
        createSimpleRecord(UUID.randomUUID().toString(), "2016-01-31T03:16:41.415Z", 12)));

    List<HoodieRecord<IndexedRecord>> taggedRecords = new FixedBucketIndex(config, location)
        .tagLocation(HoodieJavaRDD.of(recordRDD), context, HoodieSparkTable.create(config, context, metaClient)).collectAsList();

    assertEquals(location.getFileId(), taggedRecords.get(0).getCurrentLocation().getFileId());
  }

  /**
   * Maps every record to bucket 0 of every partition through the overridable hooks.
   */
  private static class FixedBucketIndex extends HoodieSimpleBucketIndex {
    private final HoodieRecordLocation location;

    FixedBucketIndex(HoodieWriteConfig config, HoodieRecordLocation location) {
      super(config);
      this.location = location;
    }

    @Override
    public int getBucketID(HoodieKey key, int numBuckets) {
      return 0;
    }

    @Override
    public Map<Integer, HoodieRecordLocation> loadBucketIdToFileIdMappingForPartition(HoodieTable hoodieTable, String partition) {
      return Collections.singletonMap(0, location);
    }
  }

  private static Object readField(Object target, Class<?> declaringClass, String name) throws ReflectiveOperationException {
    Field field = declaringClass.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static byte[] javaSerialize(Object object) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(object);
    }
    return bytes.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private static <T> T javaDeserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return (T) in.readObject();
    }
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
