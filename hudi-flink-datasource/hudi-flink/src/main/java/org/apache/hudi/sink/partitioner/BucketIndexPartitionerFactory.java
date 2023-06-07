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

import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;

/**
 * A factory class for {@link Partitioner} of bucket index table.
 */
public class BucketIndexPartitionerFactory {

  public static Partitioner instance(Configuration conf) {
    HoodieIndex.BucketIndexEngineType bucketIndexEngineType = OptionsResolver.getBucketEngineType(conf);
    switch (bucketIndexEngineType) {
      case SIMPLE:
        return new BucketIndexPartitioner(conf);
      case CONSISTENT_HASHING:
        return new ConsistentHashingBucketIndexPartitioner(conf);
      default:
        throw new HoodieNotSupportedException("Unknown bucket index engine type: " + bucketIndexEngineType);
    }
  }
}
