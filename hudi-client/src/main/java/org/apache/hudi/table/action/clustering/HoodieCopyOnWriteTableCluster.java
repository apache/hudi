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

package org.apache.hudi.table.action.clustering;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringOperation;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieCopyOnWriteTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class HoodieCopyOnWriteTableCluster implements HoodieCluster {

  private static final Logger LOG = LogManager.getLogger(HoodieCopyOnWriteTableCluster.class);
  // Accumulator to keep track of total file slices for a table
  private AccumulatorV2<Long, Long> totalFileSlices;

  public static class BaseFileIterator implements Iterator<HoodieRecord<? extends HoodieRecordPayload>> {
    List<HoodieFileReader> readers;
    Iterator<GenericRecord> currentReader;
    Schema schema;

    public BaseFileIterator(List<HoodieFileReader> readers, Schema schema) {
      this.readers = readers;
      this.schema = schema;
      if (readers.size() > 0) {
        try {
          currentReader = readers.remove(0).getRecordIterator(schema);
        } catch (Exception e) {
          throw new HoodieException(e);
        }
      }
    }

    @Override
    public boolean hasNext() {
      if (currentReader == null) {
        return false;
      } else if (currentReader.hasNext()) {
        return true;
      } else if (readers.size() > 0) {
        try {
          currentReader = readers.remove(0).getRecordIterator(schema);
          return currentReader.hasNext();
        } catch (Exception e) {
          throw new HoodieException("unable to initialize read with base file ", e);
        }
      }
      return false;
    }

    @Override
    public HoodieRecord<? extends HoodieRecordPayload> next() {
      //GenericRecord record = currentReader.next();
      return transform(currentReader.next());
    }

    private HoodieRecord<? extends HoodieRecordPayload> transform(GenericRecord record) {
      OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(Option.of(record));
      String key = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String partition = record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();

      HoodieKey hoodieKey = new HoodieKey(key, partition);
      HoodieRecord hoodieRecord = new HoodieRecord(hoodieKey, payload);
      return hoodieRecord;
    }
  }

  @Override
  public JavaRDD<WriteStatus> clustering(JavaSparkContext jsc, HoodieClusteringPlan clusteringPlan,
      HoodieTable hoodieTable, HoodieWriteConfig config, String clusteringInstantTime) throws IOException {
    if (clusteringPlan == null || (clusteringPlan.getOperations() == null)
        || (clusteringPlan.getOperations().isEmpty())) {
      return jsc.emptyRDD();
    }
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    // Compacting is very similar to applying updates to existing file
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc.hadoopConfiguration(), metaClient);
    List<ClusteringOperation> operations = clusteringPlan.getOperations().stream()
        .map(ClusteringOperation::convertFromAvroRecordInstance).collect(toList());
    LOG.info("Cluster clustering " + operations + " files");

    return jsc.parallelize(operations, operations.size())
        .map(s -> clustering(table, metaClient, config, s, clusteringInstantTime)).flatMap(List::iterator);
  }

  private List<WriteStatus> clustering(HoodieCopyOnWriteTable hoodieCopyOnWriteTable, HoodieTableMetaClient metaClient,
      HoodieWriteConfig config, ClusteringOperation operation, String instantTime) {
    Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    LOG.info("Clustering partitionPath " + operation.getPartitionPath() + " with base data files " + operation.getBaseFilePaths()
        + " for commit " + instantTime);

    List<String> baseFiles = operation.getBaseFilePaths().stream().map(
        p -> new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), operation.getPartitionPath()), p).toString())
        .collect(toList());
    List<HoodieFileReader> list = baseFiles.stream().map(s -> {
      try {
        return HoodieFileReaderFactory.getFileReader(hoodieCopyOnWriteTable.getHadoopConf(), new Path(s));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    }).collect(toList());

    BaseFileIterator baseFileIterator = new BaseFileIterator(list, readerSchema);
    Iterator<List<WriteStatus>> result = hoodieCopyOnWriteTable.handleInsert(instantTime, operation.getPartitionPath(), FSUtils.createNewFileIdPfx(),
            baseFileIterator);

    Iterable<List<WriteStatus>> resultIterable = () -> result;
    return StreamSupport.stream(resultIterable.spliterator(), false).flatMap(Collection::stream).peek(s -> {
      s.getStat().setPartitionPath(operation.getPartitionPath());
      RuntimeStats runtimeStats = new RuntimeStats();
      s.getStat().setRuntimeStats(runtimeStats);
    }).collect(toList());
  }

  @Override
  public HoodieClusteringPlan generateClusteringPlan(JavaSparkContext jsc, HoodieTable hoodieTable,
                                                     HoodieWriteConfig config, String clusteringCommitTime, Set<HoodieFileGroupId> fgIdsInPendingClusterings)
      throws IOException {


    totalFileSlices = new LongAccumulator();
    jsc.sc().register(totalFileSlices);

    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    LOG.info("Clustering " + metaClient.getBasePath() + " with commit " + clusteringCommitTime);
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
        config.shouldAssumeDatePartitioning());

    // filter the partition paths if needed to reduce list status
    partitionPaths = config.getClusteringStrategy().filterPartitionPaths(config, partitionPaths);

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no compaction plan
      return null;
    }

    SliceView fileSystemView = hoodieTable.getSliceView();
    LOG.info("Clustering looking for files to cluster in " + partitionPaths + " partitions");
    Set<HoodieFileGroupId> fgIdsPendingCompactions = ((SyncableFileSystemView) fileSystemView).getPendingCompactionOperations()
            .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
            .collect(Collectors.toSet());
    List<HoodieClusteringOperation> operations = jsc.parallelize(partitionPaths, partitionPaths.size()).map((Function<String, ClusteringOperation>) partitionPath -> {
      Stream<FileSlice> fileSliceStream = fileSystemView.getLatestFileSlices(partitionPath);
      List<HoodieBaseFile> baseFiles = fileSliceStream.filter(slice -> (!fgIdsInPendingClusterings.contains(slice.getFileGroupId()) && !fgIdsPendingCompactions.contains(slice.getFileGroupId())))
              .map(s -> s.getBaseFile()).filter(s -> s.isPresent()).map(s -> s.get()).collect(toList());
      List<String> baseFilePaths = baseFiles.stream().map(s -> s.getFileName()).collect(toList());
      /*List<String> baseFilePaths = fileSliceStream.filter(slice -> !fgIdsInPendingClusterings.contains(slice.getFileGroupId())).map(s -> {
        Option<HoodieBaseFile> baseFile = s.getBaseFile();
        totalFileSlices.add(1L);
        return baseFile;
      }).filter(c -> c.isPresent()).map(c -> c.get()).map(c -> c.getFileName()).collect(toList());*/
      Map<String, Double> metrics = config.getClusteringStrategy().captureMetrics(config, baseFiles, partitionPath);
      HoodieClusteringOperation operation =
              new HoodieClusteringOperation().newBuilder()
                      .setBaseFilePaths(baseFilePaths)
                      .setPartitionPath(partitionPath)
                      .setMetrics(metrics)
                      .build();
      return ClusteringUtils.buildClusteringOperation(operation);
    }).collect().stream().map(ClusteringUtils::buildHoodieClusteringOperation).collect(toList());
    LOG.info("Total of " + operations.size() + " clustering are retrieved");
    LOG.info("Total number of latest files slices " + totalFileSlices.value());
    // Filter the clustering with the passed in filter. This lets us choose most effective
    // clustering only
    HoodieClusteringPlan clusteringPlan = config.getClusteringStrategy().generateClusteringPlan(config, operations,
            ClusteringUtils.getAllPendingClusteringPlans(metaClient).stream().map(Pair::getValue).collect(toList()));
    List<HoodieClusteringOperation> clusteringOperations = clusteringPlan.getOperations();
    for (HoodieClusteringOperation operation : clusteringOperations) {
      for (String fileName : operation.getBaseFilePaths()) {
        String partitionPath = operation.getPartitionPath();
        String fileId = FSUtils.getFileId(fileName);
        ValidationUtils.checkArgument(!fgIdsInPendingClusterings.contains(new HoodieFileGroupId(partitionPath, fileId)),
                "Bad Clustering Plan. FileId MUST NOT have multiple pending clustering. "
                        + "Please fix your strategy implementation. FgIdsInPendingClusterings :" + fgIdsInPendingClusterings
                        + ", Selected workload :" + clusteringPlan);
      }
    }
    if (clusteringPlan.getOperations().isEmpty()) {
      LOG.warn("After filtering, Nothing to clustering for " + metaClient.getBasePath());
    }
    return clusteringPlan;
  }
}
