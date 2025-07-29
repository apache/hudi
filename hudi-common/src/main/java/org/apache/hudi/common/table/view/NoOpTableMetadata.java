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

package org.apache.hudi.common.table.view;

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Provides an implementation of {@link HoodieTableMetadata} that cannot load any file statuses.
 * This is used when building a {@link HoodieTableFileSystemView} based off of a set of pre-defined File Statuses so that only those files are considered within the view.
 */
class NoOpTableMetadata implements HoodieTableMetadata {
  @Override
  public List<StoragePathInfo> getAllFilesInPartition(StoragePath partitionPath) throws IOException {
    // This should only be called when handling empty partitions since the file statuses should be pre-loaded into the view using this class
    return Collections.emptyList();
  }

  @Override
  public List<String> getPartitionPathWithPathPrefixUsingFilterExpression(List<String> relativePathPrefixes, Types.RecordType partitionFields, Expression expression) throws IOException {
    throw new HoodieMetadataException("Unsupported operation: getPartitionPathWithPathPrefixUsingFilterExpression!");
  }

  @Override
  public List<String> getPartitionPathWithPathPrefixes(List<String> relativePathPrefixes) throws IOException {
    throw new HoodieMetadataException("Unsupported operation: getPartitionPathWithPathPrefixes!");
  }

  @Override
  public List<String> getAllPartitionPaths() throws IOException {
    throw new HoodieMetadataException("Unsupported operation: getAllPartitionPaths!");
  }

  @Override
  public Map<String, List<StoragePathInfo>> getAllFilesInPartitions(Collection<String> partitionPaths) throws IOException {
    throw new HoodieMetadataException("Unsupported operation: getAllFilesInPartitions!");
  }

  @Override
  public Option<BloomFilter> getBloomFilter(String partitionName, String fileName) throws HoodieMetadataException {
    throw new HoodieMetadataException("Unsupported operation: getBloomFilter!");
  }

  @Override
  public Option<BloomFilter> getBloomFilter(String partitionName, String fileName, String metadataPartitionName) throws HoodieMetadataException {
    throw new HoodieMetadataException("Unsupported operation: getBloomFilter!");
  }

  @Override
  public Map<Pair<String, String>, BloomFilter> getBloomFilters(List<Pair<String, String>> partitionNameFileNameList) throws HoodieMetadataException {
    throw new HoodieMetadataException("Unsupported operation: getBloomFilters!");
  }

  @Override
  public Map<Pair<String, String>, BloomFilter> getBloomFilters(List<Pair<String, String>> partitionNameFileNameList, String metadataPartitionName) throws HoodieMetadataException {
    throw new HoodieMetadataException("Unsupported operation: getBloomFilters!");
  }

  @Override
  public Map<Pair<String, String>, HoodieMetadataColumnStats> getColumnStats(List<Pair<String, String>> partitionNameFileNameList, String columnName) throws HoodieMetadataException {
    throw new HoodieMetadataException("Unsupported operation: getColumnsStats!");
  }

  @Override
  public Map<Pair<String, String>, List<HoodieMetadataColumnStats>> getColumnStats(List<Pair<String, String>> partitionNameFileNameList, List<String> columnNames) throws HoodieMetadataException {
    throw new HoodieMetadataException("Unsupported operation: getColumnsStats!");
  }

  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(HoodieData<String> recordKeys) {
    throw new HoodieMetadataException("Unsupported operation: readRecordIndex!");
  }

  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndexLocationsWithKeys(HoodieData<String> secondaryKeys, String partitionName) {
    return HoodieListPairData.eager(Collections.emptyMap());
  }

  @Override
  public HoodieData<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(
      HoodieData<? extends RawKey> rawKeys,
      String partitionName,
      boolean shouldLoadInMemory) {
    throw new HoodieMetadataException("Unsupported operation: getRecordsByKeyPrefixes!");
  }

  @Override
  public Option<String> getSyncedInstantTime() {
    throw new HoodieMetadataException("Unsupported operation: getSyncedInstantTime!");
  }

  @Override
  public Option<String> getLatestCompactionTime() {
    throw new HoodieMetadataException("Unsupported operation: getLatestCompactionTime!");
  }

  @Override
  public void reset() {

  }

  @Override
  public int getNumFileGroupsForPartition(MetadataPartitionType partition) {
    throw new HoodieMetadataException("Unsupported operation: getNumFileGroupsForPartition!");
  }

  @Override
  public Map<Pair<String, StoragePath>, List<StoragePathInfo>> listPartitions(List<Pair<String, StoragePath>> partitionPathList) throws IOException {
    throw new HoodieMetadataException("Unsupported operation: listPartitions!");
  }

  @Override
  public void close() throws Exception {

  }
}
