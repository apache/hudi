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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieBroadcast;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.index.HoodieIndexUtils.tagAsNewRecordIfNeeded;

/**
 * Simple bucket index implementation, with fixed bucket number.
 */
@Slf4j
public class HoodieSimpleBucketIndex extends HoodieBucketIndex {

  public HoodieSimpleBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  /**
   * Tags the records through a {@link BucketLocationLoader} shared with the tasks by
   * {@link HoodieEngineContext#broadcast}. Subclasses keep the per-partition path of
   * {@link HoodieBucketIndex#tagLocation}, so their overrides of {@link #getBucketID},
   * {@link #loadBucketIdToFileIdMappingForPartition} and {@link #getIndexLocationFunctionForPartition}
   * still apply.
   */
  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    if (getClass() != HoodieSimpleBucketIndex.class) {
      return super.tagLocation(records, context, hoodieTable);
    }
    return tagLocation(records, context.broadcast(new BucketLocationLoader(hoodieTable, indexKeyFields)));
  }

  /**
   * Static so that the tasks capture only the loader handle, not the index or the table.
   */
  private static <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieBroadcast<BucketLocationLoader> locationLoader) {
    return records.mapPartitions(iterator -> new MappingIterator<>(iterator,
        record -> tagAsNewRecordIfNeeded(record, locationLoader.value().getLocation(record))), false);
  }

  public Map<Integer, HoodieRecordLocation> loadBucketIdToFileIdMappingForPartition(
      HoodieTable hoodieTable,
      String partition) {
    HoodieActiveTimeline hoodieActiveTimeline = hoodieTable.getMetaClient().reloadActiveTimeline();
    Set<String> pendingInstants = hoodieActiveTimeline.filterInflights().getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toSet());
    return loadBucketIdToFileIdMapping(partition, HoodieIndexUtils.getLatestFileSlicesForPartition(partition, hoodieTable),
        bucketId -> findConflictInstantsInPartition(hoodieTable, partition, bucketId, pendingInstants));
  }

  private static Map<Integer, HoodieRecordLocation> loadBucketIdToFileIdMapping(
      String partition, List<FileSlice> latestFileSlices, Function<Integer, List<String>> conflictInstantsFinder) {
    // bucketId -> fileIds
    Map<Integer, HoodieRecordLocation> bucketIdToFileIdMapping = new HashMap<>();
    latestFileSlices
        .forEach(fileSlice -> {
          String fileId = fileSlice.getFileId();
          String commitTime = fileSlice.getBaseInstantTime();

          int bucketId = BucketIdentifier.bucketIdFromFileId(fileId);
          if (!bucketIdToFileIdMapping.containsKey(bucketId)) {
            bucketIdToFileIdMapping.put(bucketId, new HoodieRecordLocation(commitTime, fileId));
          } else {
            // Finding the instants which conflict with the bucket id
            List<String> instants = conflictInstantsFinder.apply(bucketId);

            // Check if bucket data is valid
            throw new HoodieIOException("Find multiple files at partition path="
                + partition + " that belong to the same bucket id = " + bucketId
                + ", these instants need to rollback: " + instants.toString()
                + ", you can use 'rollback_to_instant' procedure to revert the conflicts.");
          }
        });
    return bucketIdToFileIdMapping;
  }

  /**
   * Find out the conflict instants with given partition and bucket id.
   */
  public List<String> findConflictInstantsInPartition(HoodieTable hoodieTable, String partition, int bucketId, Set<String> pendingInstants) {
    return findConflictInstantsInPartition(hoodieTable.getMetaClient(), hoodieTable.getSliceView(), partition, bucketId, pendingInstants);
  }

  private static List<String> findConflictInstantsInPartition(HoodieTableMetaClient metaClient, TableFileSystemView.SliceView sliceView,
                                                              String partition, int bucketId, Set<String> pendingInstants) {
    List<String> instants = new ArrayList<>();
    StoragePath partitionPath = new StoragePath(metaClient.getBasePath(), partition);

    List<StoragePathInfo> filesInPartition = listFilesFromPartition(metaClient, partitionPath);

    Stream<FileSlice> latestFileSlicesIncludingInflight = sliceView.getLatestFileSlicesIncludingInflight(partition);
    List<String> candidates = latestFileSlicesIncludingInflight.map(FileSlice::getLatestInstantTime)
        .filter(pendingInstants::contains)
        .collect(Collectors.toList());

    for (String i : candidates) {
      if (hasPendingDataFiles(filesInPartition, i, bucketId)) {
        instants.add(i);
      }
    }
    return instants;
  }

  private static List<StoragePathInfo> listFilesFromPartition(HoodieTableMetaClient metaClient, StoragePath partitionPath) {
    try {
      return metaClient.getStorage().listFiles(partitionPath);
    } catch (IOException e) {
      // ignore the exception though
      return Collections.emptyList();
    }
  }

  public Boolean hasPendingDataFilesForInstant(List<StoragePathInfo> filesInPartition, String instant, int bucketId) {
    return hasPendingDataFiles(filesInPartition, instant, bucketId);
  }

  private static boolean hasPendingDataFiles(List<StoragePathInfo> filesInPartition, String instant, int bucketId) {
    for (StoragePathInfo status : filesInPartition) {
      String fileName = status.getPath().getName();

      try {
        if (status.isFile() && BucketIdentifier.bucketIdFromFileId(fileName) == bucketId && fileName.contains(instant)) {
          return true;
        }
      } catch (NumberFormatException e) {
        log.warn("File is not bucket file {}", fileName);
      }
    }
    return false;
  }

  public int getBucketID(HoodieKey key, int numBuckets) {
    return BucketIdentifier.getBucketId(key.getRecordKey(), indexKeyFields, numBuckets);
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  protected Function<HoodieRecord, Option<HoodieRecordLocation>> getIndexLocationFunctionForPartition(HoodieTable table, String partitionPath) {
    return new SimpleBucketIndexLocationFunction(table, partitionPath);
  }

  /**
   * Loads the bucket id to location mapping of a partition as of the write's timeline, and caches it for
   * the tasks sharing this loader.
   *
   * <p>The driver shares it through {@link HoodieEngineContext#broadcast}. Where the table is at hand (the
   * driver, or an engine that runs tasks in the driver's JVM) it reads the table's file system view; in a
   * worker of a distributed engine it builds one view from the same configs and meta client, shared by the
   * worker's tasks.
   */
  static class BucketLocationLoader implements Serializable {
    private static final long serialVersionUID = 1L;

    private final HoodieTableMetaClient metaClient;
    private final HoodieMetadataConfig metadataConfig;
    private final FileSystemViewStorageConfig viewStorageConfig;
    private final HoodieCommonConfig commonConfig;
    private final List<String> indexKeyFields;
    private final NumBucketsFunction numBucketsFunction;
    private final Option<String> latestCompletedCommitTime;
    private final Set<String> pendingInstants;
    private final transient HoodieTable table;
    private transient volatile SyncableFileSystemView workerView;
    private transient volatile ConcurrentHashMap<String, Map<Integer, HoodieRecordLocation>> partitionToBucketLocations;

    BucketLocationLoader(HoodieTable table, List<String> indexKeyFields) {
      this.table = table;
      this.metaClient = table.getMetaClient();
      HoodieWriteConfig config = table.getConfig();
      this.metadataConfig = config.getMetadataConfig();
      this.viewStorageConfig = config.getViewStorageConfig();
      this.commonConfig = config.getCommonConfig();
      this.indexKeyFields = indexKeyFields;
      this.numBucketsFunction = NumBucketsFunction.fromWriteConfig(config);
      this.latestCompletedCommitTime = metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant()
          .map(HoodieInstant::requestedTime);
      this.pendingInstants = metaClient.getActiveTimeline().filterInflights().getInstantsAsStream()
          .map(HoodieInstant::requestedTime).collect(Collectors.toSet());
    }

    Option<HoodieRecordLocation> getLocation(HoodieRecord record) {
      String partitionPath = record.getPartitionPath();
      int bucketId = BucketIdentifier.getBucketId(
          record.getRecordKey(), indexKeyFields, numBucketsFunction.getNumBuckets(partitionPath));
      return Option.ofNullable(getBucketIdToLocation(partitionPath).get(bucketId));
    }

    Map<Integer, HoodieRecordLocation> getBucketIdToLocation(String partition) {
      ConcurrentHashMap<String, Map<Integer, HoodieRecordLocation>> cache = partitionToBucketLocations;
      if (cache == null) {
        synchronized (this) {
          cache = partitionToBucketLocations;
          if (cache == null) {
            cache = new ConcurrentHashMap<>();
            partitionToBucketLocations = cache;
          }
        }
      }
      return cache.computeIfAbsent(partition, this::loadBucketIdToLocation);
    }

    private Map<Integer, HoodieRecordLocation> loadBucketIdToLocation(String partition) {
      SyncableFileSystemView view = getView();
      List<FileSlice> latestFileSlices = latestCompletedCommitTime
          .map(commitTime -> view.getLatestFileSlicesBeforeOrOn(partition, commitTime, true).collect(Collectors.toList()))
          .orElseGet(Collections::emptyList);
      return loadBucketIdToFileIdMapping(partition, latestFileSlices,
          bucketId -> findConflictInstantsInPartition(metaClient, view, partition, bucketId, pendingInstants));
    }

    /**
     * Returns the table's view where the table is at hand, otherwise one view per worker. The worker view
     * reads the metadata table like {@link HoodieTable#getViewManager()}: its reader opens the metadata
     * files per lookup and closes them after, so the view holds no open files between lookups.
     */
    private SyncableFileSystemView getView() {
      if (table != null) {
        return table.getHoodieView();
      }
      SyncableFileSystemView view = workerView;
      if (view == null) {
        synchronized (this) {
          view = workerView;
          if (view == null) {
            HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
            view = FileSystemViewManager.createViewManager(engineContext, metadataConfig, viewStorageConfig, commonConfig,
                    client -> client.getTableFormat().getMetadataFactory().create(
                        engineContext, client.getStorage(), metadataConfig, client.getBasePath().toString()))
                .getFileSystemView(metaClient);
            workerView = view;
          }
        }
      }
      return view;
    }

    @VisibleForTesting
    SyncableFileSystemView getWorkerView() {
      return workerView;
    }
  }

  private class SimpleBucketIndexLocationFunction implements Function<HoodieRecord, Option<HoodieRecordLocation>> {
    private final Map<Integer, HoodieRecordLocation> bucketIdToFileIdMapping;
    private final NumBucketsFunction numBucketsFunction;

    public SimpleBucketIndexLocationFunction(HoodieTable table, String partitionPath) {
      this.bucketIdToFileIdMapping = loadBucketIdToFileIdMappingForPartition(table, partitionPath);
      HoodieWriteConfig writeConfig = table.getConfig();
      this.numBucketsFunction = NumBucketsFunction.fromWriteConfig(writeConfig);
    }

    @Override
    public Option<HoodieRecordLocation> apply(HoodieRecord record) {
      int bucketId = getBucketID(record.getKey(), numBucketsFunction.getNumBuckets(record.getPartitionPath()));
      return Option.ofNullable(bucketIdToFileIdMapping.get(bucketId));
    }
  }
}
