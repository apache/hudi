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

package org.apache.hudi.bootstrap;

import org.apache.hudi.AbstractHoodieClient;
import org.apache.hudi.bootstrap.BootstrapWriteStatus.BootstrapSourceInfo;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.SerializableConfiguration;
import org.apache.hudi.common.consolidated.CompositeMapFile;
import org.apache.hudi.common.consolidated.CompositeMapFile.DataPayloadWriter;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieExternalFileIdMapping;
import org.apache.hudi.common.model.HoodiePartitionExternalDataFiles;
import org.apache.hudi.common.model.HoodieRollingStat;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.HoodieWriteConfig.BootstrapMode;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;

import com.codahale.metrics.Timer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieBootstrapClient extends AbstractHoodieClient {

  private static Logger logger = LogManager.getLogger(HoodieBootstrapClient.class);

  private final HoodieWriteConfig writeConfig;
  private final transient HoodieMetrics metrics;
  private transient Timer.Context writeContext = null;

  public HoodieBootstrapClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig) {
    super(jsc, new HoodieWriteConfig.Builder().withProps(clientConfig.getProps())
        .withWriteStatusClass(BootstrapWriteStatus.class).build());
    this.writeConfig = clientConfig;
    this.metrics = new HoodieMetrics(config, config.getTableName());
  }

  public HoodieBootstrapClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      Option<EmbeddedTimelineService> timelineServer) {
    super(jsc, clientConfig, timelineServer);
    this.writeConfig = clientConfig;
    this.metrics = new HoodieMetrics(config, config.getTableName());
  }

  public void bootstrap() throws IOException {
    writeContext = metrics.getCommitCtx();
    HoodieTableMetaClient metaClient = createMetaClient(false);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    metaClient.getActiveTimeline().createNewInstant(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, HoodieTimeline.BOOTSTRAP_INSTANT_TS));

    Map<BootstrapMode, List<Pair<String, List<String>>>> partitionSelections =
        listSourcePartitionsAndTagBootstrapMode(metaClient);
    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.BOOTSTRAP_INSTANT_TS), Option.empty());
    JavaRDD<BootstrapWriteStatus> writeStatuses =
        runMetadataBootstrap(table, partitionSelections.get(BootstrapMode.METADATA_ONLY_BOOTSTRAP));
    List<Pair<BootstrapSourceInfo, HoodieWriteStat>> bootstrapSourceAndStats =
        writeStatuses.map(BootstrapWriteStatus::getBootstrapSourceAndWriteStat).collect();
    List<List<Pair<BootstrapSourceInfo, HoodieWriteStat>>> sourceStatsByPartition = new ArrayList<>(
        bootstrapSourceAndStats.stream().map(sourceStatPair -> Pair.of(sourceStatPair.getRight().getPartitionPath(),
            sourceStatPair)).collect(Collectors.groupingBy(Pair::getKey,
            Collectors.mapping(x -> x.getValue(), Collectors.toList()))).values());

    final CompositeMapFile bootstrapMetadataStorage = new CompositeMapFile("bootstrap",
        new SerializableConfiguration(metaClient.getHadoopConf()), metaClient.getConsistencyGuardConfig(),
        metaClient.getMetaBootstrapPath(), metaClient.getMetaBootstrapIndexPath());
    List<Pair<String, Pair<String, Long>>> partitionToFileOfssets = jsc.parallelize(sourceStatsByPartition,
        config.getBootstrapMetadataWriterParallelism()).mapPartitions(
        (FlatMapFunction<Iterator<List<Pair<BootstrapSourceInfo, HoodieWriteStat>>>,
            Pair<String, Pair<String, Long>>>) listIterator -> {
            DataPayloadWriter writer = bootstrapMetadataStorage.getDataPayloadWriter(
                HoodieTimeline.BOOTSTRAP_INSTANT_TS, config.getBootstrapMetadataWriterMaxFileSizeInBytes());
            try {
              while (listIterator.hasNext()) {
                List<Pair<BootstrapSourceInfo, HoodieWriteStat>> sourceAndStat = listIterator.next();
                HoodiePartitionExternalDataFiles externalDataFile = generateExternalDataFileMapping(sourceAndStat);
                //TODO : See if we need serialization which support evolving schema ?
                writer.update(externalDataFile.getHoodiePartitionPath(),
                    SerializationUtils.serialize(externalDataFile));
              }
              return writer.getWrittenFileNameOffsets().iterator();
            } finally {
              if (null != writer) {
                writer.close();
              }
            }
            }, true).collect();

    // Write Index to bootstrap metadata
    final CompositeMapFile.IndexWriter indexWriter = bootstrapMetadataStorage.getIndexWriter(null,
        HoodieTimeline.BOOTSTRAP_INSTANT_TS);
    indexWriter.addIndexEntries(partitionToFileOfssets.stream());
    indexWriter.commit();
    logger.info("Bootstrap Metadata Index written !!");

    commit(HoodieTimeline.BOOTSTRAP_INSTANT_TS, sourceStatsByPartition.stream()
        .flatMap(sourceStat -> sourceStat.stream().map(s -> s.getRight())).collect(Collectors.toList()),
        Option.empty(), HoodieTimeline.COMMIT_ACTION);
  }

  private HoodiePartitionExternalDataFiles generateExternalDataFileMapping(
      List<Pair<BootstrapSourceInfo, HoodieWriteStat>> sourceAndStats) {

    if (null != sourceAndStats && !sourceAndStats.isEmpty()) {
      List<HoodieExternalFileIdMapping> externalFileIdMappings =
          sourceAndStats.stream().map(sourceAndStat -> new HoodieExternalFileIdMapping(
              sourceAndStat.getKey().getFileName(), new Path(sourceAndStat.getRight().getPath()).getName()))
              .collect(Collectors.toList());

      BootstrapSourceInfo sourceInfo = sourceAndStats.get(0).getKey();
      String hoodiePartitionPath = sourceAndStats.get(0).getRight().getPartitionPath();
      return new HoodiePartitionExternalDataFiles(sourceInfo.getBootstrapBasePath(),
          sourceInfo.getBootstrapPartitionPath(), hoodiePartitionPath, externalFileIdMappings);
    }
    return null;
  }

  private Map<BootstrapMode, List<Pair<String, List<String>>>> listSourcePartitionsAndTagBootstrapMode(
      HoodieTableMetaClient metaClient) throws IOException {
    List<Pair<String, List<String>>> folders =
        FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(),
            writeConfig.getBootstrapSourceBasePath(), new PathFilter() {
              @Override
              public boolean accept(Path path) {
                return path.getName().endsWith(".parquet");
              }
            });

    BootstrapPartitionSelector selector =
        (BootstrapPartitionSelector) ReflectionUtils.loadClass(writeConfig.getPartitionSelectorClass(),
            writeConfig);
    return selector.select(folders);
  }

  private JavaRDD<BootstrapWriteStatus> runMetadataBootstrap(HoodieTable table, List<Pair<String, List<String>>> partitions) {
    if (null == partitions || partitions.isEmpty()) {
      return jsc.emptyRDD();
    }

    BootstrapKeyGenerator keyGenerator = new BootstrapKeyGenerator(writeConfig);

    return jsc.parallelize(partitions.stream().flatMap(p -> p.getValue().stream().map(f -> Pair.of(p.getLeft(), f)))
        .collect(Collectors.toList()), writeConfig.getBootstrapParallelism()).map(partitionFilePathPair -> {
          BootstrapSourceInfo sourceInfo = new BootstrapSourceInfo(writeConfig.getBootstrapSourceBasePath(),
              partitionFilePathPair.getLeft(), partitionFilePathPair.getValue());
          return table.handleMetadataBootstrap(sourceInfo, partitionFilePathPair.getLeft(), keyGenerator);
        });
  }

  private boolean commit(String commitTime, List<HoodieWriteStat> stats,
      Option<Map<String, String>> extraMetadata, String actionType) {

    logger.info("Commiting " + commitTime);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    updateMetadataAndRollingStats(actionType, metadata, stats);

    // Finalize write
    finalizeWrite(table, commitTime, stats);

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }

    try {
      System.out.println("Hoodie Write Stats=" + metadata.toJsonString());
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, commitTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

      if (writeContext != null) {
        long durationInMs = metrics.getDurationInMs(writeContext.stop());
        metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(commitTime).getTime(), durationInMs,
            metadata, actionType);
        writeContext = null;
      }
      logger.info("Committed " + commitTime);
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + commitTime,
          e);
    } catch (ParseException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + commitTime
          + "Instant time is not of valid format", e);
    }
    return true;
  }

  private void updateMetadataAndRollingStats(String actionType, HoodieCommitMetadata metadata,
      List<HoodieWriteStat> writeStats) {
    // TODO : make sure we cannot rollback / archive last commit file
    try {
      // Create a Hoodie table which encapsulated the commits and files visible
      HoodieTable table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
      // 0. All of the rolling stat management is only done by the DELTA commit for MOR and COMMIT for COW other wise
      // there may be race conditions
      HoodieRollingStatMetadata rollingStatMetadata = new HoodieRollingStatMetadata(actionType);
      // 1. Look up the previous compaction/commit and get the HoodieCommitMetadata from there.
      // 2. Now, first read the existing rolling stats and merge with the result of current metadata.

      // Need to do this on every commit (delta or commit) to support COW and MOR.

      for (HoodieWriteStat stat : writeStats) {
        String partitionPath = stat.getPartitionPath();
        // TODO: why is stat.getPartitionPath() null at times here.
        metadata.addWriteStat(partitionPath, stat);
        HoodieRollingStat hoodieRollingStat = new HoodieRollingStat(stat.getFileId(),
            stat.getNumWrites() - (stat.getNumUpdateWrites() - stat.getNumDeletes()), stat.getNumUpdateWrites(),
            stat.getNumDeletes(), stat.getTotalWriteBytes());
        rollingStatMetadata.addRollingStat(partitionPath, hoodieRollingStat);
      }
      // The last rolling stat should be present in the completed timeline
      Option<HoodieInstant> lastInstant =
          table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            table.getActiveTimeline().getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
        Option<String> lastRollingStat = Option
            .ofNullable(commitMetadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY));
        if (lastRollingStat.isPresent()) {
          rollingStatMetadata = rollingStatMetadata
              .merge(HoodieCommitMetadata.fromBytes(lastRollingStat.get().getBytes(), HoodieRollingStatMetadata.class));
        }
      }
      metadata.addMetadata(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY, rollingStatMetadata.toJsonString());
    } catch (IOException io) {
      throw new HoodieCommitException("Unable to save rolling stats");
    }
  }

  private void finalizeWrite(HoodieTable table, String instantTime, List<HoodieWriteStat> stats) {
    try {
      final Timer.Context finalizeCtx = metrics.getFinalizeCtx();
      table.finalizeWrite(jsc, instantTime, stats);
      if (finalizeCtx != null) {
        Option<Long> durationInMs = Option.of(metrics.getDurationInMs(finalizeCtx.stop()));
        durationInMs.ifPresent(duration -> {
          logger.info("Finalize write elapsed time (milliseconds): " + duration);
          metrics.updateFinalizeWriteMetrics(duration, stats.size());
        });
      }
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }
}
