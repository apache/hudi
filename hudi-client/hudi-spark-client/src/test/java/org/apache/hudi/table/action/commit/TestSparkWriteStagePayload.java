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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.deltacommit.SparkUpsertDeltaCommitActionExecutor;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.Dependency;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.rdd.RDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;
import scala.collection.JavaConverters;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.testutils.TaskPayloadTestUtils.HEAVY_TYPES;
import static org.apache.hudi.testutils.TaskPayloadTestUtils.serializedClasses;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks what the upsert write stage ships to executors: the Spark partitioner, which is serialized
 * into every task on both sides of the shuffle, and the write function.
 */
class TestSparkWriteStagePayload extends HoodieClientTestBase {

  private static Stream<Arguments> tableAndIndexTypes() {
    return Stream.of(HoodieTableType.values()).flatMap(tableType -> Stream.of(
        Arguments.of(tableType, HoodieIndex.IndexType.SIMPLE), Arguments.of(tableType, HoodieIndex.IndexType.BUCKET)));
  }

  @ParameterizedTest
  @MethodSource("tableAndIndexTypes")
  void testUpsertStagePayload(HoodieTableType tableType, HoodieIndex.IndexType indexType) throws Exception {
    initMetaClient(tableType);
    HoodieWriteConfig.Builder configBuilder = getConfigBuilder(HoodieIndex.IndexType.SIMPLE);
    if (indexType == HoodieIndex.IndexType.BUCKET) {
      Properties keyProps = new Properties();
      keyProps.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
      configBuilder
          .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(keyProps).withIndexType(indexType).withIndexKeyField("_row_key")
              .withBucketNum("2").withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE).build())
          .withLayoutConfig(HoodieLayoutConfig.newBuilder().withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
              .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build());
    }
    HoodieWriteConfig config = configBuilder.build();
    List<HoodieRecord> inserts = dataGen.generateInserts("001", 200);
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String firstInstant = client.startCommit();
    client.commit(firstInstant, client.insert(jsc.parallelize(inserts, 2), firstInstant));

    List<HoodieRecord> records = new ArrayList<>(dataGen.generateUpdates("002", inserts.subList(0, 100)));
    records.addAll(dataGen.generateInserts("002", 100));
    HoodieData<HoodieRecord> inputRecords = context.parallelize(records, 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    HoodieData<HoodieRecord> taggedRecords = table.getIndex().tagLocation(inputRecords, context, table);
    BaseSparkCommitActionExecutor executor = tableType == HoodieTableType.COPY_ON_WRITE
        ? new SparkUpsertCommitActionExecutor(context, config, table, "002", inputRecords)
        : new SparkUpsertDeltaCommitActionExecutor(context, config, table, "002", inputRecords);

    WorkloadProfile profile = executor.prepareWorkloadProfile(taggedRecords);
    Partitioner partitioner = executor.getPartitioner(profile);
    RDD<WriteStatus> writeStage = HoodieJavaRDD.getJavaRDD(executor.mapPartitionsAsRDD(taggedRecords, partitioner)).rdd();
    ShuffleDependency<?, ?, ?> shuffle = findShuffleDependency(writeStage);
    LOG.info("{} {} upsert stage payload bytes: partitioner {}, shuffle map stage {}, write stage {}", tableType, indexType,
        serializedSize(partitioner), serializedSize(new Tuple2<>(shuffle.rdd(), shuffle)), serializedSize(writeStage));
    Map<Object, Boolean> shipped = serializedObjects(writeStage);
    assertAll(
        () -> assertPartitionerPayload(partitioner, taggedRecords.collectAsList()),
        () -> assertFalse(shipped.containsKey(HoodieJavaRDD.getJavaRDD(inputRecords).rdd()), "Write stage ships the input records RDD"),
        () -> assertFalse(shipped.containsKey(profile), "Write stage ships the workload profile"));

    // run the write: small file corrections, which the MOR executor decides in the write tasks, must still merge
    String secondInstant = client.startCommit();
    List<WriteStatus> statuses = client.upsert(jsc.parallelize(records, 2), secondInstant).collect();
    assertTrue(statuses.stream().noneMatch(WriteStatus::hasErrors));
    client.commit(secondInstant, jsc.parallelize(statuses, 1));
    if (indexType != HoodieIndex.IndexType.SIMPLE) {
      return;
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable latestTable = HoodieSparkTable.create(config, context, metaClient);
    for (String partitionPath : DEFAULT_PARTITION_PATHS) {
      List<FileSlice> slices = latestTable.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
      assertTrue(slices.stream().allMatch(slice -> slice.getLogFiles().count() == 0),
          "Updates to small files are merged into new base files, not appended to log files: " + slices);
    }
  }

  /**
   * The partitioner must not carry the table, the config or the workload profile, and a copy of it
   * that went through Java serialization must route every record to the same bucket.
   */
  private static void assertPartitionerPayload(Partitioner partitioner, List<HoodieRecord> records) throws Exception {
    Set<String> heavy = serializedClasses(partitioner).stream()
        .filter(clazz -> HEAVY_TYPES.stream().anyMatch(heavyType -> heavyType.isAssignableFrom(clazz))
            || WorkloadProfile.class.isAssignableFrom(clazz))
        .map(Class::getName)
        .collect(Collectors.toCollection(TreeSet::new));
    assertTrue(heavy.isEmpty(), "Partitioner carries " + heavy);

    Partitioner copy = roundTrip(partitioner);
    assertEquals(partitioner.numPartitions(), copy.numPartitions());
    for (HoodieRecord record : records) {
      Tuple2<?, ?> key = new Tuple2<>(record.getKey(), Option.ofNullable(record.getCurrentLocation()));
      assertEquals(partitioner.getPartition(key), copy.getPartition(key), "Bucket of " + record.getKey());
    }
  }

  private static ShuffleDependency<?, ?, ?> findShuffleDependency(RDD<?> rdd) {
    Deque<RDD<?>> toVisit = new ArrayDeque<>(Collections.singletonList(rdd));
    while (!toVisit.isEmpty()) {
      for (Dependency<?> dependency : JavaConverters.seqAsJavaList(toVisit.pop().dependencies())) {
        if (dependency instanceof ShuffleDependency) {
          return (ShuffleDependency<?, ?, ?>) dependency;
        }
        toVisit.push(dependency.rdd());
      }
    }
    throw new IllegalStateException("No shuffle in the lineage of " + rdd);
  }

  private static Map<Object, Boolean> serializedObjects(Object root) throws IOException {
    Map<Object, Boolean> objects = new IdentityHashMap<>();
    try (ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream()) {
      {
        enableReplaceObject(true);
      }

      @Override
      protected Object replaceObject(Object obj) {
        objects.put(obj, Boolean.TRUE);
        return obj;
      }
    }) {
      out.writeObject(root);
    }
    return objects;
  }

  private static int serializedSize(Object root) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(root);
    }
    return bytes.size();
  }

  @SuppressWarnings("unchecked")
  private static <T> T roundTrip(T value) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(value);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }
}
