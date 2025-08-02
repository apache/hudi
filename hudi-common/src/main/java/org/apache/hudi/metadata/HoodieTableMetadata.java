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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Interface that supports querying various pieces of metadata about a hudi table.
 */
public interface HoodieTableMetadata extends Serializable, AutoCloseable {

  Logger LOG = LoggerFactory.getLogger(HoodieTableMetadata.class);

  // Table name suffix
  String METADATA_TABLE_NAME_SUFFIX = "_metadata";
  /**
   * Timestamp for a commit when the base dataset had not had any commits yet. this is < than even
   * {@link org.apache.hudi.common.table.timeline.HoodieTimeline#INIT_INSTANT_TS}, such that the metadata table
   * can be prepped even before bootstrap is done.
   */
  String SOLO_COMMIT_TIMESTAMP = "00000000000000";
  // Key for the record which saves list of all partitions
  String RECORDKEY_PARTITION_LIST = "__all_partitions__";
  // The partition name used for non-partitioned tables
  String NON_PARTITIONED_NAME = ".";
  String EMPTY_PARTITION_NAME = "";

  /**
   * Return the base-path of the Metadata Table for the given Dataset identified by base-path
   */
  static String getMetadataTableBasePath(String dataTableBasePath) {
    return dataTableBasePath + StoragePath.SEPARATOR + HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH;
  }

  /**
   * Return the base-path of the Metadata Table for the given Dataset identified by base-path
   */
  static StoragePath getMetadataTableBasePath(StoragePath dataTableBasePath) {
    return new StoragePath(dataTableBasePath, HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH);
  }

  /**
   * Returns the base path of the Dataset provided the base-path of the Metadata Table of this
   * Dataset
   */
  static String getDataTableBasePathFromMetadataTable(String metadataTableBasePath) {
    checkArgument(isMetadataTable(metadataTableBasePath));
    return metadataTableBasePath.substring(0, metadataTableBasePath.lastIndexOf(HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH) - 1);
  }

  /**
   * Return the base path of the dataset.
   *
   * @param metadataTableBasePath The base path of the metadata table
   */
  static String getDatasetBasePath(String metadataTableBasePath) {
    int endPos = metadataTableBasePath.lastIndexOf(StoragePath.SEPARATOR + HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH);
    checkState(endPos != -1, metadataTableBasePath + " should be base path of the metadata table");
    return metadataTableBasePath.substring(0, endPos);
  }

  /**
   * Returns {@code True} if the given path contains a metadata table.
   *
   * @param basePath The base path to check
   */
  static boolean isMetadataTable(String basePath) {
    if (basePath == null || basePath.isEmpty()) {
      return false;
    }
    if (basePath.endsWith(StoragePath.SEPARATOR)) {
      basePath = basePath.substring(0, basePath.length() - 1);
    }
    return basePath.endsWith(HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH);
  }

  static boolean isMetadataTable(StoragePath basePath) {
    return isMetadataTable(basePath.toString());
  }

  /**
   * Fetch all the files at the given partition path, per the latest snapshot of the metadata.
   */
  List<StoragePathInfo> getAllFilesInPartition(StoragePath partitionPath) throws IOException;

  /**
   * Retrieve the paths of partitions under the provided sub-directories,
   * and try to filter these partitions using the provided {@link Expression}.
   */
  List<String> getPartitionPathWithPathPrefixUsingFilterExpression(List<String> relativePathPrefixes,
                                                                   Types.RecordType partitionFields,
                                                                   Expression expression) throws IOException;

  /**
   * Fetches all partition paths that are the sub-directories of the list of provided (relative) paths.
   * <p>
   * E.g., Table has partition 4 partitions:
   * year=2022/month=08/day=30, year=2022/month=08/day=31, year=2022/month=07/day=03, year=2022/month=07/day=04
   * The relative path "year=2022" returns all partitions, while the relative path
   * "year=2022/month=07" returns only two partitions.
   */
  List<String> getPartitionPathWithPathPrefixes(List<String> relativePathPrefixes) throws IOException;

  /**
   * Fetch list of all partition paths, per the latest snapshot of the metadata.
   */
  List<String> getAllPartitionPaths() throws IOException;

  /**
   * Fetch all files for given partition paths.
   *
   * NOTE: Absolute partition paths are expected here
   */
  Map<String, List<StoragePathInfo>> getAllFilesInPartitions(Collection<String> partitionPaths)
      throws IOException;

  /**
   * Get the bloom filter for the FileID from the metadata table.
   *
   * @param partitionName - Partition name
   * @param fileName      - File name for which bloom filter needs to be retrieved
   * @return BloomFilter if available, otherwise empty
   * @throws HoodieMetadataException
   */
  default Option<BloomFilter> getBloomFilter(final String partitionName, final String fileName)
      throws HoodieMetadataException {
    return getBloomFilter(partitionName, fileName, MetadataPartitionType.BLOOM_FILTERS.getPartitionPath());
  }

  /**
   * Get the bloom filter for the FileID from the metadata table.
   *
   * @param partitionName         - Partition name
   * @param fileName              - File name for which bloom filter needs to be retrieved
   * @param metadataPartitionName - Metadata partition name
   * @return BloomFilter if available, otherwise empty
   * @throws HoodieMetadataException
   */
  Option<BloomFilter> getBloomFilter(final String partitionName, final String fileName, final String metadataPartitionName)
      throws HoodieMetadataException;

  /**
   * Get bloom filters for files from the metadata table index.
   *
   * @param partitionNameFileNameList - List of partition and file name pair for which bloom filters need to be retrieved
   * @return Map of partition file name pair to its bloom filter
   * @throws HoodieMetadataException
   */
  default Map<Pair<String, String>, BloomFilter> getBloomFilters(final List<Pair<String, String>> partitionNameFileNameList)
      throws HoodieMetadataException {
    return getBloomFilters(partitionNameFileNameList, MetadataPartitionType.BLOOM_FILTERS.getPartitionPath());
  }

  /**
   * Get bloom filters for files from the metadata table index.
   *
   * @param partitionNameFileNameList - List of partition and file name pair for which bloom filters need to be retrieved
   * @param metadataPartitionName     - Metadata partition name
   * @return Map of partition file name pair to its bloom filter
   * @throws HoodieMetadataException
   */
  Map<Pair<String, String>, BloomFilter> getBloomFilters(final List<Pair<String, String>> partitionNameFileNameList, final String metadataPartitionName)
      throws HoodieMetadataException;

  /**
   * Get column stats for files from the metadata table index.
   *
   * @param partitionNameFileNameList - List of partition and file name pair for which bloom filters need to be retrieved
   * @param columnName                - Column name for which stats are needed
   * @return Map of partition and file name pair to its column stats
   * @throws HoodieMetadataException
   */
  Map<Pair<String, String>, HoodieMetadataColumnStats> getColumnStats(final List<Pair<String, String>> partitionNameFileNameList, final String columnName)
      throws HoodieMetadataException;

  /**
   * Get column stats for files from the metadata table index.
   *
   * @param partitionNameFileNameList - List of partition and file name pair for which bloom filters need to be retrieved.
   * @param columnNames               - List of column name for which stats are needed.
   * @return Map of partition and file name pair to a list of column stats.
   * @throws HoodieMetadataException
   */
  Map<Pair<String, String>, List<HoodieMetadataColumnStats>> getColumnStats(List<Pair<String, String>> partitionNameFileNameList, List<String> columnNames)
      throws HoodieMetadataException;

  /**
   * Returns pairs of (record key, location of record key) which are found in the record index.
   * Records that are not found are ignored and wont be part of map object that is returned.
   */
  HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(HoodieData<String> recordKeys);

  /**
   * Returns pairs of (record key, location of record key) which are found in the record index.
   * Records that are not found are ignored and wont be part of map object that is returned.
   * @param recordKeys list of recordkeys to look up in the index
   * @param dataTablePartition option of the data table partition to look up from. This is only applicable to partitioned rli
   *                           record keys are not globally unique so the record key alone does not sufficiently identify an
   *                           individual record.
   * @return map from recordkey to the location of the record that was read from the index
   *
   */
  HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(HoodieData<String> recordKeys, Option<String> dataTablePartition);

  /**
   * Returns the location of record keys which are found in the record index.
   * Records that are not found are ignored and wont be part of map object that is returned.
   */
  default HoodieData<HoodieRecordGlobalLocation> readRecordIndexLocations(HoodieData<String> recordKeys) {
    return readRecordIndexLocationsWithKeys(recordKeys).values();
  }

  /**
   * Returns pairs of (secondary key, location of secondary key) which the provided secondary keys maps to.
   * Records that are not found are ignored and won't be part of map object that is returned.
   */
  HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndexLocationsWithKeys(HoodieData<String> secondaryKeys, String partitionName);

  /**
   * Returns the location of secondary keys which are found in the secondary index.
   * Records that are not found are ignored and won't be part of map object that is returned.
   */
  default HoodieData<HoodieRecordGlobalLocation> readSecondaryIndexLocations(HoodieData<String> secondaryKeys, String partitionName) {
    return readSecondaryIndexLocationsWithKeys(secondaryKeys, partitionName).values();
  }

  /**
   * Fetch records by key prefixes. The raw keys are encoded using their encode() method to generate
   * the actual key prefixes used for lookup in the metadata table partitions.
   *
   * @param rawKeys           list of raw key objects to be encoded into key prefixes
   * @param partitionName     partition name in metadata table where the records are looked up for
   * @param shouldLoadInMemory whether to load records in memory
   * @return {@link HoodieData} of {@link HoodieRecord}s with records matching the encoded key prefixes
   */
  HoodieData<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(
      HoodieData<? extends RawKey> rawKeys,
      String partitionName,
      boolean shouldLoadInMemory);

  /**
   * Get the instant time to which the metadata is synced w.r.t data timeline.
   */
  Option<String> getSyncedInstantTime();

  /**
   * Returns the timestamp of the latest compaction.
   */
  Option<String> getLatestCompactionTime();

  /**
   * Clear the states of the table metadata.
   */
  void reset();

  /**
   * Returns the number of shards in a metadata table partition.
   */
  int getNumFileGroupsForPartition(MetadataPartitionType partition);

  /**
   * Get file groups in the record index partition grouped by bucket
   * @param partition the metadata table partition type
   * @return map from data table partition to list of filegroups that index that partition
   */
  Map<String, List<FileSlice>> getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType partition);

  /**
   * @param partitionPathList A list of pairs of the relative and absolute paths of the partitions.
   * @return all the files from the partitions.
   * @throws IOException upon error.
   */
  Map<Pair<String, StoragePath>, List<StoragePathInfo>> listPartitions(List<Pair<String, StoragePath>> partitionPathList) throws IOException;
}
