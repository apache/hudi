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

package org.apache.hudi.table;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieBroadcast;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.BaseTableServicePlanActionExecutor;
import org.apache.hudi.table.action.clean.CleanActionExecutor;
import org.apache.hudi.table.action.cluster.strategy.ClusteringPlanStrategy;
import org.apache.hudi.table.action.cluster.strategy.PartitionAwareClusteringPlanStrategy;
import org.apache.hudi.table.action.compact.plan.generators.BaseHoodieCompactionPlanGenerator;
import org.apache.hudi.table.action.compact.plan.generators.HoodieCompactionPlanGenerator;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.testutils.BroadcastReleaseTracker;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.TaskFileAccessRecordingFileSystem;

import example.plugin.CustomClusteringPlanStrategy;
import example.plugin.CustomCompactionPlanGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.testutils.TaskPayloadTestUtils.HEAVY_TYPES;
import static org.apache.hudi.testutils.TaskPayloadTestUtils.serializedClasses;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Checks what the table services ship to executors: the task functions they hand to the engine context must not
 * carry the table, the write config, the meta client or the object that runs the service.
 */
class TestTableServiceTaskPayload extends HoodieClientTestBase {

  @Test
  void testCleanTaskPayload() throws Exception {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = getConfigBuilder()
        .withCleanConfig(HoodieCleanConfig.newBuilder().withAutoClean(false)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1).build())
        .build();
    HoodieSparkEngineContext spyContext = spy(context);
    BroadcastReleaseTracker broadcasts = BroadcastReleaseTracker.install(spyContext);
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(spyContext, config)) {
      List<HoodieRecord> records = write(client, null);
      write(client, records);
      write(client, records);
      assertTrue(client.clean().getTotalFilesDeleted() > 0);
    }
    ArgumentCaptor<SerializablePairFlatMapFunction> captor = ArgumentCaptor.forClass(SerializablePairFlatMapFunction.class);
    verify(spyContext, atLeastOnce()).mapPartitionsToPairAndReduceByKey(any(), captor.capture(), any(), anyInt());
    assertLightPayload("clean delete", ownedBy(captor.getAllValues(), CleanActionExecutor.class), CleanActionExecutor.class);
    broadcasts.assertAllReleased();
  }

  @Test
  void testDeleteInvalidFilesTaskPayload() throws Exception {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = getConfigBuilder().withFinalizeWriteParallelism(200).build();
    List<String> invalidFiles = Arrays.asList(
        DEFAULT_PARTITION_PATHS[0] + "/f1.parquet", DEFAULT_PARTITION_PATHS[0] + "/f2.parquet", DEFAULT_PARTITION_PATHS[1] + "/f3.parquet");
    for (String file : invalidFiles) {
      storage.create(new StoragePath(basePath, file)).close();
    }
    WriteMarkers markers = mock(WriteMarkers.class);
    when(markers.doesMarkerDirExist()).thenReturn(true);
    when(markers.createdAndMergedDataPaths(any(), anyInt())).thenReturn(new HashSet<>(invalidFiles));
    HoodieSparkEngineContext spyContext = spy(context);
    BroadcastReleaseTracker broadcasts = BroadcastReleaseTracker.install(spyContext);
    HoodieTable table = HoodieSparkTable.create(config, spyContext, metaClient);

    table.reconcileAgainstMarkers(spyContext, "001", Collections.emptyList(), true, false, markers);

    for (String file : invalidFiles) {
      assertFalse(storage.exists(new StoragePath(basePath, file)), file);
    }
    ArgumentCaptor<SerializableFunction> functions = ArgumentCaptor.forClass(SerializableFunction.class);
    ArgumentCaptor<Integer> parallelisms = ArgumentCaptor.forClass(Integer.class);
    // wait for the files to appear, delete them, wait for them to disappear
    verify(spyContext, times(3)).map(anyList(), functions.capture(), parallelisms.capture());
    assertEquals(Arrays.asList(2, invalidFiles.size(), 2), parallelisms.getAllValues(), "One task per partition or file");
    assertLightPayload("invalid file reconciliation", functions.getAllValues());
    assertEquals(3, broadcasts.created());
    broadcasts.assertAllReleased();
  }

  @Test
  void testClusteringPlanTaskPayload() throws Exception {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = getClusteringConfig(Option.empty(), false);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      write(client, null);
      write(client, null);
    }
    // a pending clustering plan on the first partition: its file groups are not eligible again
    HoodieWriteConfig firstPartitionConfig = getClusteringConfig(Option.of(DEFAULT_FIRST_PARTITION_PATH), false);
    try (SparkRDDWriteClient client = getHoodieWriteClient(firstPartitionConfig)) {
      assertTrue(client.scheduleClustering(Option.empty()).isPresent());
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieSparkEngineContext spyContext = spy(context);
    BroadcastReleaseTracker broadcasts = BroadcastReleaseTracker.install(spyContext);
    List<HoodieClusteringGroup> groups = generateClusteringPlan(config, spyContext).getInputGroups();
    ArgumentCaptor<SerializableFunction> captor = ArgumentCaptor.forClass(SerializableFunction.class);
    verify(spyContext, atLeastOnce()).map(anyList(), captor.capture(), anyInt());
    assertLightPayload("clustering plan", ownedBy(captor.getAllValues(), PartitionAwareClusteringPlanStrategy.class), ClusteringPlanStrategy.class);
    assertEquals(1, broadcasts.created());
    broadcasts.assertAllReleased();

    Set<String> partitions = groups.stream().flatMap(group -> group.getSlices().stream())
        .map(HoodieSliceInfo::getPartitionPath).collect(Collectors.toCollection(TreeSet::new));
    Set<String> expectedPartitions = new TreeSet<>(Arrays.asList(DEFAULT_PARTITION_PATHS));
    expectedPartitions.remove(DEFAULT_FIRST_PARTITION_PATH);
    assertEquals(expectedPartitions, partitions);
    List<HoodieClusteringGroup> localGroups = generateClusteringPlan(getClusteringConfig(Option.empty(), true), context).getInputGroups();
    assertEquals(localGroups, groups, "The plan must not depend on where the groups are built");
  }

  @Test
  void testClusteringPlanReadsPendingFileGroupsOnce() throws Exception {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = getClusteringConfig(Option.empty(), true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      write(client, null);
      write(client, null);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = spy(HoodieSparkTable.create(config, context, metaClient));
    SyncableFileSystemView view = spy((SyncableFileSystemView) table.getSliceView());
    doReturn(view).when(table).getSliceView();
    HoodieClusteringPlan plan = new SparkSizeBasedClusteringPlanStrategy<>(table, new HoodieLocalEngineContext(storageConf), config)
        .generateClusteringPlan(null, Lazy.eagerly(Arrays.asList(DEFAULT_PARTITION_PATHS))).get();
    assertEquals(DEFAULT_PARTITION_PATHS.length, plan.getInputGroups().size());
    verify(view, times(1)).getPendingCompactionOperations();
    verify(view, times(1)).getPendingLogCompactionOperations();
    verify(view, times(1)).getFileGroupsInPendingClustering();
  }

  @Test
  void testCompactionPlanTaskPayload() throws Exception {
    List<?> functions = scheduleCompactionAndCaptureTaskFunctions(Option.empty());
    assertLightPayload("compaction plan", functions, BaseHoodieCompactionPlanGenerator.class);
  }

  /**
   * A plan generator outside the Hudi packages may keep per-call state, so each task gets its own copy of it.
   */
  @Test
  void testCustomCompactionPlanGeneratorIsCopiedPerTask() throws Exception {
    List<?> functions = scheduleCompactionAndCaptureTaskFunctions(Option.of(CustomCompactionPlanGenerator.class.getName()));
    for (Object function : functions) {
      assertPerTaskCopy(function, CustomCompactionPlanGenerator.class);
    }
  }

  @Test
  void testCustomClusteringPlanStrategyIsCopiedPerTask() throws Exception {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = getClusteringConfig(Option.empty(), false);
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      write(client, null);
      write(client, null);
    }
    HoodieSparkEngineContext spyContext = spy(context);
    BroadcastReleaseTracker broadcasts = BroadcastReleaseTracker.install(spyContext);
    HoodieTable table = HoodieSparkTable.create(config, spyContext, HoodieTableMetaClient.reload(metaClient));
    List<HoodieClusteringGroup> groups = new CustomClusteringPlanStrategy<>(table, spyContext, config)
        .generateClusteringPlan(null, Lazy.eagerly(Arrays.asList(DEFAULT_PARTITION_PATHS))).get().getInputGroups();

    ArgumentCaptor<SerializableFunction> captor = ArgumentCaptor.forClass(SerializableFunction.class);
    verify(spyContext, atLeastOnce()).map(anyList(), captor.capture(), anyInt());
    for (Object function : ownedBy(captor.getAllValues(), PartitionAwareClusteringPlanStrategy.class)) {
      assertPerTaskCopy(function, CustomClusteringPlanStrategy.class);
    }
    assertEquals(0, broadcasts.created());
    assertEquals(generateClusteringPlan(config, context).getInputGroups(), groups);
  }

  /**
   * The planning tasks run on a deserialized copy of the strategy, shared by the tasks of the executor, and read
   * nothing under the meta folder.
   */
  @Test
  void testClusteringPlanTasksRunOnExecutorCopy() throws Exception {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = getClusteringConfig(Option.empty(), false);
    RecordingClusteringPlanStrategy<?> driverStrategy;
    List<HoodieClusteringGroup> groups;
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      write(client, null);
      write(client, null);
      RecordingClusteringPlanStrategy.INSTANCES.clear();
      TaskFileAccessRecordingFileSystem.reset();
      // the client's config reads the file system view from its timeline server, as the tasks of a write do
      HoodieTable table = HoodieSparkTable.create(client.getConfig(), context, createFileAccessRecordingMetaClient());
      driverStrategy = new RecordingClusteringPlanStrategy<>(table, context, client.getConfig());
      groups = driverStrategy.generateClusteringPlan(null, Lazy.eagerly(Arrays.asList(DEFAULT_PARTITION_PATHS))).get().getInputGroups();
    }

    assertEquals(generateClusteringPlan(config, context).getInputGroups(), groups);
    assertExecutorCopies(RecordingClusteringPlanStrategy.INSTANCES, driverStrategy);
    RecordingClusteringPlanStrategy.INSTANCES.forEach(strategy -> assertNull(strategy.getEngineContext()));
    assertNoTaskMetaFolderAccess();
  }

  /**
   * The planning tasks run on a deserialized copy of the generator, shared by the tasks of the executor, and read
   * nothing under the meta folder.
   */
  @Test
  void testCompactionPlanTasksRunOnExecutorCopy() throws Exception {
    initMetaClient(HoodieTableType.MERGE_ON_READ);
    Properties props = new Properties();
    props.setProperty(HoodieCompactionConfig.COMPACTION_PLAN_GENERATOR.key(), RecordingCompactionPlanGenerator.class.getName());
    HoodieWriteConfig config = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().fromProperties(props).withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
    HoodieCompactionPlan plan;
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      write(client, write(client, null));
      RecordingCompactionPlanGenerator.INSTANCES.clear();
      TaskFileAccessRecordingFileSystem.reset();
      // the client's config reads the file system view from its timeline server, as the tasks of a write do
      HoodieTable table = HoodieSparkTable.create(client.getConfig(), context, createFileAccessRecordingMetaClient());
      plan = (HoodieCompactionPlan) table.scheduleCompaction(context, table.getMetaClient().createNewInstantTime(false), Option.empty()).get();
    }

    assertEquals(DEFAULT_PARTITION_PATHS.length, plan.getOperations().size());
    assertExecutorCopies(RecordingCompactionPlanGenerator.INSTANCES, null);
    RecordingCompactionPlanGenerator.INSTANCES.forEach(generator -> assertNull(generator.engineContext()));
    assertNoTaskMetaFolderAccess();
  }

  private HoodieTableMetaClient createFileAccessRecordingMetaClient() {
    StorageConfiguration<?> conf = storageConf.newInstance();
    TaskFileAccessRecordingFileSystem.register(conf.unwrapAs(Configuration.class));
    return HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).build();
  }

  private static void assertExecutorCopies(Collection<?> taskInstances, Object driverInstance) {
    assertFalse(taskInstances.isEmpty(), "No planning task ran");
    Set<Object> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
    distinct.addAll(taskInstances);
    assertFalse(distinct.contains(driverInstance), "A task ran on the driver instance");
    assertEquals(1, distinct.size(), "The tasks of the executor share one copy");
  }

  private void assertNoTaskMetaFolderAccess() {
    String metaFolder = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME).toUri().getPath();
    List<TaskFileAccessRecordingFileSystem.Access> accesses = new ArrayList<>(TaskFileAccessRecordingFileSystem.taskOpens());
    accesses.addAll(TaskFileAccessRecordingFileSystem.taskListings());
    assertTrue(accesses.stream().noneMatch(access -> access.getPath().startsWith(metaFolder)), "Tasks read the meta folder: " + accesses);
  }

  private List<?> scheduleCompactionAndCaptureTaskFunctions(Option<String> planGeneratorClass) throws IOException {
    initMetaClient(HoodieTableType.MERGE_ON_READ);
    Properties props = new Properties();
    planGeneratorClass.ifPresent(className -> props.setProperty(HoodieCompactionConfig.COMPACTION_PLAN_GENERATOR.key(), className));
    HoodieWriteConfig config = getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().fromProperties(props).withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
    HoodieSparkEngineContext spyContext = spy(context);
    BroadcastReleaseTracker broadcasts = BroadcastReleaseTracker.install(spyContext);
    String compactionInstant;
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(spyContext, config)) {
      write(client, write(client, null));
      compactionInstant = (String) client.scheduleCompaction(Option.empty()).get();
    }
    HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(HoodieTableMetaClient.reload(metaClient), compactionInstant);
    assertEquals(DEFAULT_PARTITION_PATHS.length, plan.getOperations().size());
    ArgumentCaptor<SerializableFunction> captor = ArgumentCaptor.forClass(SerializableFunction.class);
    verify(spyContext, atLeastOnce()).flatMap(anyList(), captor.capture(), anyInt());
    broadcasts.assertAllReleased();
    return ownedBy(captor.getAllValues(), BaseHoodieCompactionPlanGenerator.class);
  }

  private static void assertPerTaskCopy(Object function, Class<?> customClass) {
    Set<Class<?>> classes = serializedClasses(function);
    assertTrue(classes.contains(customClass), "Each task carries its own " + customClass.getSimpleName());
    assertTrue(classes.stream().noneMatch(HoodieBroadcast.class::isAssignableFrom), "No broadcast of " + customClass.getSimpleName());
  }

  @Test
  void testMetadataFileGroupInitializationTaskPayload() throws Exception {
    initMetaClient(HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = getConfigBuilder()
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();
    HoodieSparkEngineContext spyContext = spy(context);
    BroadcastReleaseTracker broadcasts = BroadcastReleaseTracker.install(spyContext);
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(spyContext, config)) {
      write(client, null);
    }
    ArgumentCaptor<SerializableConsumer> captor = ArgumentCaptor.forClass(SerializableConsumer.class);
    verify(spyContext, atLeastOnce()).foreach(anyList(), captor.capture(), anyInt());
    assertLightPayload("metadata file group initialization", ownedBy(captor.getAllValues(), HoodieBackedTableMetadataWriter.class),
        HoodieBackedTableMetadataWriter.class);
    broadcasts.assertAllReleased();
  }

  private HoodieWriteConfig getClusteringConfig(Option<String> partitionSelected, boolean useLocalEngineContext) {
    HoodieClusteringConfig.Builder clusteringConfig = HoodieClusteringConfig.newBuilder()
        .withClusteringPlanStrategyClass(SparkSizeBasedClusteringPlanStrategy.class.getName())
        .withClusteringPlanSmallFileLimit(Long.MAX_VALUE)
        .useLocalEngineContextForPlanGeneration(useLocalEngineContext);
    partitionSelected.ifPresent(clusteringConfig::withClusteringPartitionSelected);
    // no small file packing, so each insert adds a file group to every partition
    return getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0).build())
        .withClusteringConfig(clusteringConfig.build())
        .build();
  }

  private HoodieClusteringPlan generateClusteringPlan(HoodieWriteConfig config, HoodieSparkEngineContext engineContext) {
    HoodieTable table = HoodieSparkTable.create(config, engineContext, HoodieTableMetaClient.reload(metaClient));
    return new SparkSizeBasedClusteringPlanStrategy<>(table, engineContext, config)
        .generateClusteringPlan(null, Lazy.eagerly(Arrays.asList(DEFAULT_PARTITION_PATHS))).get();
  }

  /**
   * Inserts new records, or updates {@code toUpdate} when given, and returns the records written.
   */
  private List<HoodieRecord> write(SparkRDDWriteClient client, List<HoodieRecord> toUpdate) throws IOException {
    String instantTime = client.startCommit();
    List<HoodieRecord> records = toUpdate == null ? dataGen.generateInserts(instantTime, 30) : dataGen.generateUpdates(instantTime, toUpdate);
    JavaRDD<HoodieRecord> input = jsc.parallelize(records, 2);
    JavaRDD<WriteStatus> statuses = toUpdate == null ? client.insert(input, instantTime) : client.upsert(input, instantTime);
    assertTrue(client.commit(instantTime, statuses));
    return records;
  }

  /**
   * Returns the functions defined by {@code owner}, among those handed to the engine context.
   */
  private static List<?> ownedBy(List<?> functions, Class<?> owner) {
    List<?> owned = functions.stream()
        .filter(function -> function.getClass().getName().startsWith(owner.getName() + "$$Lambda"))
        .collect(Collectors.toList());
    assertFalse(owned.isEmpty(), "No function of " + owner.getName() + " in " + functions);
    return owned;
  }

  private static void assertLightPayload(String name, List<?> functions, Class<?>... moreHeavyTypes) throws IOException {
    for (Object function : functions) {
      assertLightPayload(name, function, moreHeavyTypes);
    }
  }

  private static void assertLightPayload(String name, Object function, Class<?>... moreHeavyTypes) throws IOException {
    LOG.info("{} task function: {} bytes", name, serializedSize(function));
    List<Class<?>> heavyTypes = Stream.concat(HEAVY_TYPES.stream(), Stream.of(moreHeavyTypes)).collect(Collectors.toList());
    Set<String> heavy = serializedClasses(function).stream()
        .filter(clazz -> heavyTypes.stream().anyMatch(heavyType -> heavyType.isAssignableFrom(clazz)))
        .map(Class::getName)
        .collect(Collectors.toCollection(TreeSet::new));
    assertTrue(heavy.isEmpty(), name + " task function carries " + heavy);
  }

  private static int serializedSize(Object root) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(root);
    }
    return bytes.size();
  }

  /**
   * Records the instances that build the clustering groups of a partition.
   */
  public static class RecordingClusteringPlanStrategy<T> extends SparkSizeBasedClusteringPlanStrategy<T> {
    static final Queue<RecordingClusteringPlanStrategy<?>> INSTANCES = new ConcurrentLinkedQueue<>();

    public RecordingClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
      super(table, engineContext, writeConfig);
    }

    @Override
    protected Pair<Stream<HoodieClusteringGroup>, Boolean> buildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
      INSTANCES.add(this);
      return super.buildClusteringGroupsForPartition(partitionPath, fileSlices);
    }

    @Override
    public HoodieEngineContext getEngineContext() {
      return super.getEngineContext();
    }
  }

  /**
   * Records the instances that select the file slices to compact.
   */
  public static class RecordingCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O> extends HoodieCompactionPlanGenerator<T, I, K, O> {
    static final Queue<RecordingCompactionPlanGenerator<?, ?, ?, ?>> INSTANCES = new ConcurrentLinkedQueue<>();

    public RecordingCompactionPlanGenerator(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig,
                                            BaseTableServicePlanActionExecutor executor) {
      super(table, engineContext, writeConfig, executor);
    }

    @Override
    protected boolean filterFileSlice(FileSlice fileSlice, String lastCompletedInstantTime, Set<HoodieFileGroupId> pendingFileGroupIds,
                                      Option<InstantRange> instantRange) {
      INSTANCES.add(this);
      return super.filterFileSlice(fileSlice, lastCompletedInstantTime, pendingFileGroupIds, instantRange);
    }

    HoodieEngineContext engineContext() {
      return engineContext;
    }
  }
}
