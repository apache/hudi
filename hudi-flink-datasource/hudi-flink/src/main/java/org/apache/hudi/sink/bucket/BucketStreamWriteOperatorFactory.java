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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.common.WriteOperatorFactory;

import org.apache.flink.configuration.Configuration;

/**
 * BucketStreamWrite Operator Factory
 */
public class BucketStreamWriteOperatorFactory {

  public static <I> WriteOperatorFactory<I> getFactory(Configuration conf) {
    String bucketEngineType = conf.get(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE);
    if (bucketEngineType.equalsIgnoreCase(HoodieIndex.BucketIndexEngineType.SIMPLE.name())) {
      return WriteOperatorFactory.instance(conf, new SimpleBucketStreamWriteOperator<>(conf));
    } else if (bucketEngineType.equalsIgnoreCase(HoodieIndex.BucketIndexEngineType.CONSISTENT_HASHING.name())) {
      return WriteOperatorFactory.instance(conf, new ConsistentBucketStreamWriteOperator<>(conf));
    } else {
      throw new HoodieException("Unknown bucket index engine type: " + bucketEngineType);
    }
  }
}
