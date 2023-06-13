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

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.common.AbstractWriteOperator;
import org.apache.hudi.sink.common.WriteOperatorFactory;

import org.apache.flink.configuration.Configuration;

/**
 * Factory class for {@link WriteOperatorFactory} with bucket index.
 */
public class BucketStreamWriteOperatorFactories {

  public static <I> WriteOperatorFactory<I> instance(Configuration conf) {
    HoodieIndex.BucketIndexEngineType bucketIndexEngineType = OptionsResolver.getBucketEngineType(conf);
    final AbstractWriteOperator<I> writeOperator;
    switch (bucketIndexEngineType) {
      case SIMPLE:
        writeOperator = new BucketStreamWriteOperator<>(conf);
        break;
      case CONSISTENT_HASHING:
        validate(conf);
        writeOperator = new ConsistentBucketStreamWriteOperator<>(conf);
        break;
      default:
        throw new HoodieNotSupportedException("Unknown bucket index engine type: " + bucketIndexEngineType);
    }
    return WriteOperatorFactory.instance(conf, writeOperator);
  }

  private static void validate(Configuration conf) {
    String writeOperation = conf.get(FlinkOptions.OPERATION);
    switch (WriteOperationType.fromValue(writeOperation)) {
      case INSERT:
      case UPSERT:
      case DELETE:
        break;
      case INSERT_OVERWRITE:
      case INSERT_OVERWRITE_TABLE:
        throw new HoodieException("Consistent hashing bucket index does not work with insert overwrite using FLINK engine. Use simple bucket index or Spark engine.");
      default:
        throw new HoodieNotSupportedException("Unsupported write operation : " + writeOperation);
    }
  }
}
