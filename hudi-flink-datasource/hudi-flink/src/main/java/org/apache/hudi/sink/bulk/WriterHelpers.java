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

package org.apache.hudi.sink.bulk;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.bucket.BucketBulkInsertWriterHelper;
import org.apache.hudi.table.HoodieTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;

/**
 * Factory clazz to generate bulk insert writer helpers.
 */
public class WriterHelpers {
  public static BulkInsertWriterHelper getWriterHelper(Configuration conf, HoodieTable<?, ?, ?, ?> hoodieTable, HoodieWriteConfig writeConfig,
                                                       String instantTime, int taskPartitionId, long taskId, long taskEpochId, RowType rowType) {
    return OptionsResolver.isBucketIndexType(conf)
        ? new BucketBulkInsertWriterHelper(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType)
        : new BulkInsertWriterHelper(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType);
  }
}
