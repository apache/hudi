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

package org.apache.hudi.source.prune;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A function to fetch the bucket id for buckets pruning.
 */
public class PartitionBucketIdFunc implements Function<String, Integer>, Serializable {
  private final NumBucketsFunction numBucketsFunction;   // partition -> numBuckets
  private final Function<Integer, Integer> bucketIdFunc; // numBuckets -> bucketId

  private PartitionBucketIdFunc(NumBucketsFunction numBucketsFunction, Function<Integer, Integer> bucketIdFunc) {
    this.numBucketsFunction = numBucketsFunction;
    this.bucketIdFunc = bucketIdFunc;
  }

  /**
   * Creates the {@code PartitionBucketIdFunc} instance, will return null if the bucket pruning does not take effect.
   *
   * @param bucketIdFuncOpt   The bucket id function option
   * @param metaClient        The meta client
   * @param defaultBucketsNum The default num buckets
   */
  @Nullable
  public static PartitionBucketIdFunc create(Option<Function<Integer, Integer>> bucketIdFuncOpt, HoodieTableMetaClient metaClient, int defaultBucketsNum) {
    if (bucketIdFuncOpt.isPresent()) {
      return new PartitionBucketIdFunc(NumBucketsFunction.fromMetaClient(metaClient, defaultBucketsNum), bucketIdFuncOpt.get());
    } else {
      return null;
    }
  }

  /**
   * Returns the bucket id for the given partition path.
   */
  @Override
  public Integer apply(String partitionPath) {
    return this.bucketIdFunc.apply(this.numBucketsFunction.getNumBuckets(partitionPath));
  }
}