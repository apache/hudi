/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.bootstrap;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.bootstrap.BootstrapWriteStatus;
import org.apache.hudi.client.bootstrap.FullRecordBootstrapDataProvider;
import org.apache.hudi.client.bootstrap.HoodieBootstrapSchemaProvider;
import org.apache.hudi.client.bootstrap.HoodieSparkBootstrapSchemaProvider;
import org.apache.hudi.client.bootstrap.selector.BootstrapModeSelector;
import org.apache.hudi.client.bootstrap.translator.BootstrapPartitionPathTranslator;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.SparkValidatorUtils;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.KeyGeneratorInterface;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BaseCommitActionExecutor;
import org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkBulkInsertCommitActionExecutor;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE;
import static org.apache.hudi.table.action.bootstrap.MetadataBootstrapHandlerFactory.getMetadataHandler;

public class SparkBootstrapCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseCommitActionExecutor<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>, HoodieBootstrapWriteMetadata<HoodieData<WriteStatus>>> {

  private static final Logger LOG = LogManager.getLogger(SparkBootstrapCommitActionExecutor.class);
  protected String bootstrapSchema = null;
  private transient FileSystem bootstrapSourceFileSystem;

  public SparkBootstrapCommitActionExecutor(HoodieSparkEngineContext context,
                                            HoodieWriteConfig config,
                                            HoodieTable table,
                                            Option<Map<String, String>> extraMetadata) {
    super(context, new HoodieWriteConfig.Builder().withProps(config.getProps())
        .withAutoCommit(true).withWriteStatusClass(BootstrapWriteStatus.class)
        .withBulkInsertParallelism(config.getBootstrapParallelism())
        .build(), table, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, WriteOperationType.BOOTSTRAP,
        extraMetadata);
    bootstrapSourceFileSystem = FSUtils.getFs(config.getBootstrapSourceBasePath(), hadoopConf);
  }

  private void validate() {
    ValidationUtils.checkArgument(config.getBootstrapSourceBasePath() != null,
        "Ensure Bootstrap Source Path is set");
    ValidationUtils.checkArgument(config.getBootstrapModeSelectorClass() != null,
        "Ensure Bootstrap Partition Selector is set");
  }

  @Override
  public HoodieBootstrapWriteMetadata<HoodieData<WriteStatus>> execute() {
    validate();
    try {
      HoodieTableMetaClient metaClient = table.getMetaClient();
      Option<HoodieInstant> completedInstant =
          metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
      ValidationUtils.checkArgument(!completedInstant.isPresent(),
          "Active Timeline is expected to be empty for bootstrap to be performed. "
              + "If you want to re-bootstrap, please rollback bootstrap first !!");
      Map<BootstrapMode, List<Pair<String, List<HoodieFileStatus>>>> partitionSelections = listAndProcessSourcePartitions();

      // First run metadata bootstrap which will auto commit
      Option<HoodieWriteMetadata<HoodieData<WriteStatus>>> metadataResult = metadataBootstrap(partitionSelections.get(BootstrapMode.METADATA_ONLY));
      // if there are full bootstrap to be performed, perform that too
      Option<HoodieWriteMetadata<HoodieData<WriteStatus>>> fullBootstrapResult = fullBootstrap(partitionSelections.get(BootstrapMode.FULL_RECORD));

      return new HoodieBootstrapWriteMetadata(metadataResult, fullBootstrapResult);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  @Override
  protected String getSchemaToStoreInCommit() {
    return bootstrapSchema;
  }

  /**
   * Perform Metadata Bootstrap.
   * @param partitionFilesList List of partitions and files within that partitions
   */
  protected Option<HoodieWriteMetadata<HoodieData<WriteStatus>>> metadataBootstrap(List<Pair<String, List<HoodieFileStatus>>> partitionFilesList) {
    if (null == partitionFilesList || partitionFilesList.isEmpty()) {
      return Option.empty();
    }

    HoodieTableMetaClient metaClient = table.getMetaClient();
    String bootstrapInstantTime = HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS;
    metaClient.getActiveTimeline().createNewInstant(
        new HoodieInstant(State.REQUESTED, metaClient.getCommitActionType(), bootstrapInstantTime));

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
        metaClient.getCommitActionType(), bootstrapInstantTime), Option.empty());

    HoodieData<BootstrapWriteStatus> bootstrapWriteStatuses = runMetadataBootstrap(partitionFilesList);

    HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();
    updateIndexAndCommitIfNeeded(bootstrapWriteStatuses.map(w -> w), result);

    // Delete the marker directory for the instant
    WriteMarkersFactory.get(config.getMarkersType(), table, bootstrapInstantTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());

    return Option.of(result);
  }

  private void updateIndexAndCommitIfNeeded(HoodieData<WriteStatus> writeStatuses, HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    // cache writeStatusRDD before updating index, so that all actions before this are not triggered again for future
    // RDD actions that are performed after updating the index.
    writeStatuses.persist(config.getString(WRITE_STATUS_STORAGE_LEVEL_VALUE));
    Instant indexStartTime = Instant.now();
    // Update the index back
    HoodieData<WriteStatus> statuses = table.getIndex().updateLocation(writeStatuses, context, table);
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
    commitOnAutoCommit(result);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(HoodieData<HoodieRecord<T>> inputRecords) {
    // NO_OP
    return null;
  }

  @Override
  protected void setCommitMetadata(HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    result.setCommitMetadata(Option.of(new HoodieCommitMetadata()));
  }

  @Override
  protected void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    // Perform bootstrap index write and then commit. Make sure both record-key and bootstrap-index
    // is all done in a single job DAG.
    Map<String, List<Pair<BootstrapFileMapping, HoodieWriteStat>>> bootstrapSourceAndStats =
        result.getWriteStatuses().collectAsList().stream()
            .map(w -> {
              BootstrapWriteStatus ws = (BootstrapWriteStatus) w;
              return Pair.of(ws.getBootstrapSourceFileMapping(), ws.getStat());
            }).collect(Collectors.groupingBy(w -> w.getKey().getPartitionPath()));
    HoodieTableMetaClient metaClient = table.getMetaClient();
    try (BootstrapIndex.IndexWriter indexWriter = BootstrapIndex.getBootstrapIndex(metaClient)
        .createWriter(metaClient.getTableConfig().getBootstrapBasePath().get())) {
      LOG.info("Starting to write bootstrap index for source " + config.getBootstrapSourceBasePath() + " in table "
          + config.getBasePath());
      indexWriter.begin();
      bootstrapSourceAndStats.forEach((key, value) -> indexWriter.appendNextPartition(key,
          value.stream().map(Pair::getKey).collect(Collectors.toList())));
      indexWriter.finish();
      LOG.info("Finished writing bootstrap index for source " + config.getBootstrapSourceBasePath() + " in table "
          + config.getBasePath());
    }

    commit(extraMetadata, result, bootstrapSourceAndStats.values().stream()
        .flatMap(f -> f.stream().map(Pair::getValue)).collect(Collectors.toList()));
    LOG.info("Committing metadata bootstrap !!");
  }

  protected void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata<HoodieData<WriteStatus>> result, List<HoodieWriteStat> stats) {
    String actionType = table.getMetaClient().getCommitActionType();
    LOG.info("Committing " + instantTime + ", action Type " + actionType);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable table = HoodieSparkTable.create(config, context);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    result.setCommitted(true);
    stats.forEach(stat -> metadata.addWriteStat(stat.getPartitionPath(), stat));
    result.setWriteStats(stats);

    // Finalize write
    finalizeWrite(instantTime, stats, result);
    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, getSchemaToStoreInCommit());
    metadata.setOperationType(operationType);

    writeTableMetadata(metadata, actionType);

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      LOG.info("Committed " + instantTime);
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
    result.setCommitMetadata(Option.of(metadata));
  }

  /**
   * Perform Full Bootstrap.
   * @param partitionFilesList List of partitions and files within that partitions
   */
  protected Option<HoodieWriteMetadata<HoodieData<WriteStatus>>> fullBootstrap(List<Pair<String, List<HoodieFileStatus>>> partitionFilesList) {
    if (null == partitionFilesList || partitionFilesList.isEmpty()) {
      return Option.empty();
    }
    TypedProperties properties = new TypedProperties();
    properties.putAll(config.getProps());
    FullRecordBootstrapDataProvider inputProvider =
        (FullRecordBootstrapDataProvider) ReflectionUtils.loadClass(config.getFullBootstrapInputProvider(),
            properties, context);
    JavaRDD<HoodieRecord> inputRecordsRDD =
        (JavaRDD<HoodieRecord>) inputProvider.generateInputRecords("bootstrap_source", config.getBootstrapSourceBasePath(),
            partitionFilesList);
    // Start Full Bootstrap
    String bootstrapInstantTime = HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS;
    final HoodieInstant requested = new HoodieInstant(
        State.REQUESTED, table.getMetaClient().getCommitActionType(), bootstrapInstantTime);
    table.getActiveTimeline().createNewInstant(requested);

    // Setup correct schema and run bulk insert.
    Option<HoodieWriteMetadata<HoodieData<WriteStatus>>> writeMetadataOption =
        Option.of(getBulkInsertActionExecutor(HoodieJavaRDD.of(inputRecordsRDD)).execute());

    // Delete the marker directory for the instant
    WriteMarkersFactory.get(config.getMarkersType(), table, bootstrapInstantTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());

    return writeMetadataOption;
  }

  protected BaseSparkCommitActionExecutor<T> getBulkInsertActionExecutor(HoodieData<HoodieRecord> inputRecordsRDD) {
    return new SparkBulkInsertCommitActionExecutor((HoodieSparkEngineContext) context, new HoodieWriteConfig.Builder().withProps(config.getProps())
        .withSchema(bootstrapSchema).build(), table, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
        inputRecordsRDD, Option.empty(), extraMetadata);
  }

  /**
   * Return Bootstrap Mode selections for partitions listed and figure out bootstrap Schema.
   * @return
   * @throws IOException
   */
  private Map<BootstrapMode, List<Pair<String, List<HoodieFileStatus>>>> listAndProcessSourcePartitions() throws IOException {
    List<Pair<String, List<HoodieFileStatus>>> folders = BootstrapUtils.getAllLeafFoldersWithFiles(
            table.getMetaClient(), bootstrapSourceFileSystem, config.getBootstrapSourceBasePath(), context);

    LOG.info("Fetching Bootstrap Schema !!");
    HoodieBootstrapSchemaProvider sourceSchemaProvider = new HoodieSparkBootstrapSchemaProvider(config);
    bootstrapSchema = sourceSchemaProvider.getBootstrapSchema(context, folders).toString();
    LOG.info("Bootstrap Schema :" + bootstrapSchema);

    BootstrapModeSelector selector =
        (BootstrapModeSelector) ReflectionUtils.loadClass(config.getBootstrapModeSelectorClass(), config);

    Map<BootstrapMode, List<String>> result = selector.select(folders);
    Map<String, List<HoodieFileStatus>> partitionToFiles = folders.stream().collect(
        Collectors.toMap(Pair::getKey, Pair::getValue));

    // Ensure all partitions are accounted for
    ValidationUtils.checkArgument(partitionToFiles.keySet().equals(
        result.values().stream().flatMap(Collection::stream).collect(Collectors.toSet())));

    return result.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue().stream()
        .map(p -> Pair.of(p, partitionToFiles.get(p))).collect(Collectors.toList())))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private HoodieData<BootstrapWriteStatus> runMetadataBootstrap(List<Pair<String, List<HoodieFileStatus>>> partitions) {
    if (null == partitions || partitions.isEmpty()) {
      return context.emptyHoodieData();
    }

    TypedProperties properties = new TypedProperties();
    properties.putAll(config.getProps());

    KeyGeneratorInterface keyGenerator;
    try {
      keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(properties);
    } catch (IOException e) {
      throw new HoodieKeyGeneratorException("Init keyGenerator failed ", e);
    }

    BootstrapPartitionPathTranslator translator = (BootstrapPartitionPathTranslator) ReflectionUtils.loadClass(
        config.getBootstrapPartitionPathTranslatorClass(), properties);

    List<Pair<String, Pair<String, HoodieFileStatus>>> bootstrapPaths = partitions.stream()
        .flatMap(p -> {
          String translatedPartitionPath = translator.getBootstrapTranslatedPath(p.getKey());
          return p.getValue().stream().map(f -> Pair.of(p.getKey(), Pair.of(translatedPartitionPath, f)));
        })
        .collect(Collectors.toList());

    context.setJobStatus(this.getClass().getSimpleName(), "Run metadata-only bootstrap operation: " + config.getTableName());
    return context.parallelize(bootstrapPaths, config.getBootstrapParallelism())
        .map(partitionFsPair -> getMetadataHandler(config, table, partitionFsPair.getRight().getRight()).runMetadataBootstrap(partitionFsPair.getLeft(),
                partitionFsPair.getRight().getLeft(), keyGenerator));
  }

  @Override
  protected Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr) {
    throw new UnsupportedOperationException("Should not called in bootstrap code path");
  }

  @Override
  protected Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    throw new UnsupportedOperationException("Should not called in bootstrap code path");
  }

  @Override
  protected void runPrecommitValidators(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    SparkValidatorUtils.runValidators(config, writeMetadata, context, table, instantTime);
  }
}
