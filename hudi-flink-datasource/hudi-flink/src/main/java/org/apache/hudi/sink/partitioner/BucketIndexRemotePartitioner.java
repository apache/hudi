/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.RemotePartitionHelper;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.util.ViewStorageProperties;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * Bucket index input partitioner backed by the embedded timeline service.
 *
 * @param <T> The Hoodie key type used for bucket partitioning
 */
public class BucketIndexRemotePartitioner<T extends HoodieKey> implements Partitioner<T> {

  private final Configuration conf;
  // Index key fields, pre-parsed by the caller. The per-record partition() path uses the List
  // overload of getBucketId so the comma-separated config string is never re-split per record.
  private final List<String> indexKeyFieldList;
  private final NumBucketsFunction numBucketsFunction;

  private transient RemotePartitionHelper remotePartitionHelper;

  public BucketIndexRemotePartitioner(Configuration conf, List<String> indexKeyFieldList) {
    this.conf = conf;
    this.indexKeyFieldList = indexKeyFieldList;
    this.numBucketsFunction = new NumBucketsFunction(conf.get(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS),
        conf.get(FlinkOptions.BUCKET_INDEX_PARTITION_RULE), conf.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS));
  }

  BucketIndexRemotePartitioner(Configuration conf, List<String> indexKeyFieldList, RemotePartitionHelper remotePartitionHelper) {
    this(conf, indexKeyFieldList);
    this.remotePartitionHelper = remotePartitionHelper;
  }

  @Override
  public int partition(T key, int numPartitions) {
    String partitionPath = normalizePartitionPath(key.getPartitionPath());
    int numBuckets = numBucketsFunction.getNumBuckets(partitionPath);
    int curBucket = BucketIdentifier.getBucketId(key.getRecordKey(), indexKeyFieldList, numBuckets);
    return doGetRemotePartition(getRemotePartitionHelper(), numBuckets, partitionPath, curBucket, numPartitions);
  }

  public static int getRemotePartition(
      RemotePartitionHelper remotePartitionHelper,
      NumBucketsFunction numBucketsFunction,
      String partitionPath,
      int curBucket,
      int numPartitions) {
    String normalizedPartitionPath = normalizePartitionPath(partitionPath);
    int numBuckets = numBucketsFunction.getNumBuckets(normalizedPartitionPath);
    return doGetRemotePartition(remotePartitionHelper, numBuckets, normalizedPartitionPath, curBucket, numPartitions);
  }

  private static int doGetRemotePartition(
      RemotePartitionHelper remotePartitionHelper,
      int numBuckets,
      String partitionPath,
      int curBucket,
      int numPartitions) {
    int partition;
    try {
      partition = remotePartitionHelper.getPartition(
          numBuckets,
          partitionPath,
          curBucket,
          numPartitions);
    } catch (Exception e) {
      throw new HoodieException("Get remote partition failed.", e);
    }
    if (partition < 0) {
      throw new HoodieException(
          "Get remote partition succeeded, but the subtask id is negative: " + partition);
    }
    return partition;
  }

  private static String normalizePartitionPath(String partitionPath) {
    return partitionPath == null ? "" : partitionPath;
  }

  private RemotePartitionHelper getRemotePartitionHelper() {
    if (remotePartitionHelper == null) {
      FileSystemViewStorageConfig viewStorageConfig =
          ViewStorageProperties.loadFromProperties(conf.get(FlinkOptions.PATH), conf);
      remotePartitionHelper = new RemotePartitionHelper(viewStorageConfig);
    }
    return remotePartitionHelper;
  }
}
