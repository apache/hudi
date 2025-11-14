package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.hadoop.SerializablePath;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadataKeyGenerator;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieSparkTable;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_POPULATE_META_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.utilities.MetadataSyncUtils.getPendingInstants;
import static org.apache.hudi.utilities.MetadataSyncUtils.getTableSyncExtraMetadata;

public class HoodieBootstrapMetadataSync implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBootstrapMetadataSync.class);
  private final String sourceBasePath;
  private final transient HoodieEngineContext engineContext;

  protected HoodieBackedTableMetadata targetTableMetadata;
  protected HoodieTableMetaClient targetTableMetadataMetaClient;
  private HoodieTableMetaClient targetTableMetaClient;
  private HoodieTableMetaClient sourceTableMetaClient;
  private final Schema schema;

  private static final String RECORD_KEY_FIELD_NAME = HoodieMetadataPayload.KEY_FIELD_NAME;

  private HoodieWriteConfig writeConfig;
  protected HoodieWriteConfig metadataWriteConfig;

  public HoodieBootstrapMetadataSync(HoodieWriteConfig writeConfig, JavaSparkContext jsc, String sourceBasePath, String targetBasePath, Schema sourceSchema) {
    this.sourceBasePath = sourceBasePath;
    this.engineContext = new HoodieSparkEngineContext(jsc);
    this.writeConfig = writeConfig;
    this.targetTableMetaClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(targetBasePath).build();
    this.sourceTableMetaClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(sourceBasePath).build();
    this.schema = sourceSchema;

    this.metadataWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(writeConfig, HoodieFailedWritesCleaningPolicy.NEVER);
  }

  private void initMetadataReader() {
    if (this.targetTableMetadata != null) {
      this.targetTableMetadata.close();
    }

    try {
      this.targetTableMetadata = new HoodieBackedTableMetadata(engineContext, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), true);
      this.targetTableMetadataMetaClient = targetTableMetadata.getMetadataMetaClient();
    } catch (Exception e) {
      throw new HoodieException("Could not open MDT for reads", e);
    }
  }

  public void run() throws Exception {
    Option<HoodieInstant>  lastInstantOpt = sourceTableMetaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().lastInstant();
    if(!lastInstantOpt.isPresent()) {
      return;
    }

    String lastInstantTimestamp = lastInstantOpt.get().getTimestamp();
    try(SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, writeConfig)) {
      HoodieSparkTable sparkTable = HoodieSparkTable.create(writeConfig, engineContext, targetTableMetaClient);
      String commitTime = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, targetTableMetaClient); // single writer. will rollback any pending commits from previous round.
      targetTableMetaClient
          .reloadActiveTimeline()
          .transitionRequestedToInflight(
              HoodieTimeline.REPLACE_COMMIT_ACTION,
              commitTime);

      boolean filesPartitionAvailable = targetTableMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.FILES);
      if (!filesPartitionAvailable) {
        // Initialize the metadata table for the first time
        targetTableMetadataMetaClient = initializeMetaClient();
      } else {
        // Check and then open the metadata table reader so FILES partition can be read during initialization of other partitions
        initMetadataReader();
        // Load the metadata table metaclient if required
        if (targetTableMetadataMetaClient == null) {
          targetTableMetadataMetaClient = HoodieTableMetaClient.builder().setConf(engineContext.getHadoopConf().get()).setBasePath(metadataWriteConfig.getBasePath()).build();
        }
      }

      // initialize metadata writer
      List<HoodieBackedTableMetadataWriter.DirectoryInfo> partitionInfoList = listAllPartitionsFromFilesystem(lastInstantTimestamp, engineContext, sourceBasePath, sourceTableMetaClient);

      Pair<Integer, HoodieData<HoodieRecord>> fileGroupCountAndRecordsPair = initializeFilesPartition(partitionInfoList, engineContext, sourceBasePath);

      try {
        if (!filesPartitionAvailable) {
          initializeFileGroups(targetTableMetadataMetaClient, MetadataPartitionType.FILES, commitTime, fileGroupCountAndRecordsPair.getKey());
        }
      } catch (IOException e) {
        throw new HoodieException("Failed to bootstrap table " + sourceBasePath, e);
      }

      // Perform the commit using bulkCommit
      HoodieData<HoodieRecord> records = fileGroupCountAndRecordsPair.getValue();
      try (HoodieBackedTableMetadataWriter hoodieTableMetadataWriter =
               (HoodieBackedTableMetadataWriter) sparkTable.getMetadataWriter(commitTime).get()) {
        hoodieTableMetadataWriter.bulkCommit(commitTime, MetadataPartitionType.FILES, records, fileGroupCountAndRecordsPair.getKey(), !filesPartitionAvailable);
        targetTableMetaClient.reloadActiveTimeline();
        targetTableMetaClient.getTableConfig().setMetadataPartitionState(targetTableMetaClient, MetadataPartitionType.FILES, true);
      }

      Option<HoodieInstant> targetTableLastInstant = targetTableMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
      SyncMetadata syncMetadata = getTableSyncExtraMetadata(targetTableLastInstant, targetTableMetaClient,
          sourceBasePath, lastInstantTimestamp, getPendingInstants(sourceTableMetaClient.getActiveTimeline(), lastInstantOpt.get()));
      HoodieReplaceCommitMetadata replaceCommitMetadata = buildComprehensiveReplaceCommitMetadata();
      replaceCommitMetadata.addMetadata(SyncMetadata.TABLE_SYNC_METADATA, syncMetadata.toJson());
      targetTableMetaClient
          .reloadActiveTimeline()
          .saveAsComplete(new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, commitTime),
              Option.of(replaceCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      // initialize the metadata reader again so the MDT partition can be read after initialization
    }
  }

  public List<HoodieBackedTableMetadataWriter.DirectoryInfo> listAllPartitionsFromFilesystem(String initializationTime, HoodieEngineContext engineContext, String basePath, HoodieTableMetaClient srcMetaClient) {
    List<SerializablePath> pathsToList = new LinkedList<>();
    pathsToList.add(new SerializablePath(new CachingPath(basePath)));

    List<HoodieBackedTableMetadataWriter.DirectoryInfo> partitionsToBootstrap = new LinkedList<>();
    final int fileListingParallelism = 10; // make it configurable
    SerializableConfiguration conf = new SerializableConfiguration(srcMetaClient.getHadoopConf());
    final String datasetBasePath = srcMetaClient.getBasePath();
    SerializablePath serializableBasePath = new SerializablePath(new CachingPath(datasetBasePath));

    while (!pathsToList.isEmpty()) {
      // In each round we will list a section of directories
      int numDirsToList = Math.min(fileListingParallelism, pathsToList.size());
      // List all directories in parallel
      engineContext.setJobStatus(this.getClass().getSimpleName(), "Listing " + numDirsToList + " partitions from filesystem");
      List<HoodieBackedTableMetadataWriter.DirectoryInfo> processedDirectories = engineContext.map(pathsToList.subList(0, numDirsToList), path -> {
        FileSystem fs = path.get().getFileSystem(conf.get());
        String relativeDirPath = FSUtils.getRelativePartitionPath(serializableBasePath.get(), path.get());
        return new HoodieBackedTableMetadataWriter.DirectoryInfo(relativeDirPath, fs.listStatus(path.get()), initializationTime);
      }, numDirsToList);

      pathsToList = new LinkedList<>(pathsToList.subList(numDirsToList, pathsToList.size()));

      // If the listing reveals a directory, add it to queue. If the listing reveals a hoodie partition, add it to
      // the results.
      for (HoodieBackedTableMetadataWriter.DirectoryInfo dirInfo : processedDirectories) {

        if (dirInfo.isHoodiePartition()) {
          // Add to result
          partitionsToBootstrap.add(dirInfo);
        } else {
          // Add sub-dirs to the queue
          pathsToList.addAll(dirInfo.getSubDirectories().stream()
              .map(path -> new SerializablePath(new CachingPath(path.toUri())))
              .collect(Collectors.toList()));
        }
      }
    }

    return partitionsToBootstrap;
  }

  private Pair<Integer, HoodieData<HoodieRecord>> initializeFilesPartition(List<HoodieBackedTableMetadataWriter.DirectoryInfo> partitionInfoList, HoodieEngineContext engineContext, String basePath) {
    // FILES partition uses a single file group
    final int fileGroupCount = 1;

    List<String> partitions = partitionInfoList.stream().map(p -> HoodieTableMetadataUtil.getPartitionIdentifier(p.getRelativePath()))
        .collect(Collectors.toList());
    final int totalDataFilesCount = partitionInfoList.stream().mapToInt(HoodieBackedTableMetadataWriter.DirectoryInfo::getTotalFiles).sum();
    LOG.info("Committing total {} partitions and {} files to metadata", partitions.size(), totalDataFilesCount);

    // Record which saves the list of all partitions
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(partitions);
    HoodieData<HoodieRecord> allPartitionsRecord = engineContext.parallelize(Collections.singletonList(record), 1);
    if (partitionInfoList.isEmpty()) {
      return Pair.of(fileGroupCount, allPartitionsRecord);
    }

    // Records which save the file listing of each partition
    engineContext.setJobStatus(this.getClass().getSimpleName(), "Creating records for metadata FILES partition");

    HoodieData<HoodieRecord> fileListRecords = engineContext.parallelize(partitionInfoList, partitionInfoList.size()).map(partitionInfo -> {
      Map<String, Long> fileNameToSizeMap = partitionInfo.getFileNameToSizeMap();
      return HoodieMetadataPayload.createPartitionFilesRecord(
          HoodieTableMetadataUtil.getPartitionIdentifier(partitionInfo.getRelativePath()), fileNameToSizeMap, Collections.emptyList(),
          true, basePath);
    });
    ValidationUtils.checkState(fileListRecords.count() == partitions.size());

    return Pair.of(fileGroupCount, allPartitionsRecord.union(fileListRecords));
  }

  private void initializeFileGroups(HoodieTableMetaClient dataMetaClient, MetadataPartitionType metadataPartition, String instantTime,
                                    int fileGroupCount) throws IOException {
    // Remove all existing file groups or leftover files in the partition
    final Path partitionPath = new Path(metadataWriteConfig.getBasePath(), metadataPartition.getPartitionPath());
    FileSystem fs = targetTableMetadataMetaClient.getFs();
    try {
      final FileStatus[] existingFiles = fs.listStatus(partitionPath);
      if (existingFiles.length > 0) {
        LOG.warn("Deleting all existing files found in MDT partition " + metadataPartition.getPartitionPath());
        fs.delete(partitionPath, true);
        ValidationUtils.checkState(!fs.exists(partitionPath), "Failed to delete MDT partition " + metadataPartition);
      }
    } catch (FileNotFoundException ignored) {
      // If the partition did not exist yet, it will be created below
    }

    // Archival of data table has a dependency on compaction(base files) in metadata table.
    // It is assumed that as of time Tx of base instant (/compaction time) in metadata table,
    // all commits in data table is in sync with metadata table. So, we always start with log file for any fileGroup.

    // Even though the initial commit is a bulkInsert which creates the first baseFiles directly, we still
    // create a log file first. This ensures that if any fileGroups of the MDT index do not receive any records
    // during initial commit, then the fileGroup would still be recognized (as a FileSlice with no baseFiles but a
    // valid logFile). Since these log files being created have no content, it is safe to add them here before
    // the bulkInsert.
    final String msg = String.format("Creating %d file groups for partition %s with base fileId %s at instant time %s",
        fileGroupCount, metadataPartition.getPartitionPath(), metadataPartition.getFileIdPrefix(), instantTime);
    LOG.info(msg);
    final List<String> fileGroupFileIds = IntStream.range(0, fileGroupCount)
        .mapToObj(i -> HoodieTableMetadataUtil.getFileIDForFileGroup(metadataPartition, i))
        .collect(Collectors.toList());
    ValidationUtils.checkArgument(fileGroupFileIds.size() == fileGroupCount);
    engineContext.setJobStatus(this.getClass().getSimpleName(), msg);
    engineContext.foreach(fileGroupFileIds, fileGroupFileId -> {
      try {
        final Map<HoodieLogBlock.HeaderMetadataType, String> blockHeader = Collections.singletonMap(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
        final HoodieDeleteBlock block = new HoodieDeleteBlock(new DeleteRecord[0], blockHeader);

        HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
            .onParentPath(FSUtils.getPartitionPath(metadataWriteConfig.getBasePath(), metadataPartition.getPartitionPath()))
            .withFileId(fileGroupFileId)
            .overBaseCommit(instantTime)
            .withLogVersion(HoodieLogFile.LOGFILE_BASE_VERSION)
            .withFileSize(0L)
            .withSizeThreshold(metadataWriteConfig.getLogFileMaxSize())
            .withFs(dataMetaClient.getFs())
            .withRolloverLogWriteToken(HoodieLogFormat.DEFAULT_WRITE_TOKEN)
            .withLogWriteToken(HoodieLogFormat.DEFAULT_WRITE_TOKEN)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
        writer.appendBlock(block);
        writer.close();
      } catch (InterruptedException e) {
        throw new HoodieException("Failed to created fileGroup " + fileGroupFileId + " for partition " + metadataPartition.getPartitionPath(), e);
      }
    }, fileGroupFileIds.size());
  }

  private HoodieReplaceCommitMetadata buildComprehensiveReplaceCommitMetadata() {
    List<HoodieInstant> replaceCommits =  sourceTableMetaClient.getActiveTimeline().filterCompletedInstants().getInstants().stream()
        .filter(instant -> instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).collect(Collectors.toList());

    HoodieReplaceCommitMetadata replaceCommitMetadata = new HoodieReplaceCommitMetadata();
    Map<String, List<String>> totalPartitionToReplacedFiles = new HashMap<>();
    replaceCommits.stream().forEach(replaceCommit -> {
      try {
         HoodieReplaceCommitMetadata commitMetadata = HoodieReplaceCommitMetadata.fromBytes(
            sourceTableMetaClient.getCommitsTimeline().getInstantDetails(replaceCommit).get(), HoodieReplaceCommitMetadata.class);
         Map<String, List<String>> partitionsToReplacedFileGroups = commitMetadata.getPartitionToReplaceFileIds();
         for (Map.Entry<String, List<String>> entry : partitionsToReplacedFileGroups.entrySet()) {
           String partition = entry.getKey();
           List<String> replacedFiles = entry.getValue();
           totalPartitionToReplacedFiles.computeIfAbsent(partition, k -> new ArrayList<>());
           totalPartitionToReplacedFiles.get(partition).addAll(replacedFiles);
         }
      } catch (IOException e) {
        throw new HoodieException("Failed to deserialize instant " + replaceCommit.getTimestamp(), e);
      }
    });

    replaceCommitMetadata.setPartitionToReplaceFileIds(totalPartitionToReplacedFiles);

    replaceCommitMetadata.addMetadata("schema", schema.toString());
    replaceCommitMetadata.setOperationType(WriteOperationType.BOOTSTRAP);
    replaceCommitMetadata.setCompacted(false);
    return replaceCommitMetadata;
  }

  private HoodieTableMetaClient initializeMetaClient() throws IOException {
    return HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName(writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX)
        .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
        .setPayloadClassName(HoodieMetadataPayload.class.getName())
        .setBaseFileFormat(HoodieFileFormat.HFILE.toString())
        .setRecordKeyFields(RECORD_KEY_FIELD_NAME)
        .setPopulateMetaFields(DEFAULT_METADATA_POPULATE_META_FIELDS)
        .setKeyGeneratorClassProp(HoodieTableMetadataKeyGenerator.class.getCanonicalName())
        .initTable(engineContext.getHadoopConf().get(), metadataWriteConfig.getBasePath());
  }

}
