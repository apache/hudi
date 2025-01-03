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

package org.apache.hudi.sink.append;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.sink.bucket.BucketBulkInsertWriterHelper;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.table.HoodieTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BucketAppendWriterHelper extends BucketBulkInsertWriterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(BucketAppendWriterHelper.class);

  private final String indexKeys;
  private final int numBuckets;
  private final RowDataKeyGen keyGen;
  private Map<String, String> bucketIdToFileId;
  private boolean needFixedFileIdSuffix;

  public BucketAppendWriterHelper(Configuration conf, HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                  String instantTime, int taskPartitionId, long taskId, long taskEpochId, RowType rowType) {
    super(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType);
    this.indexKeys = OptionsResolver.getIndexKeyField(conf);
    this.numBuckets = conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
    this.keyGen = RowDataKeyGen.instance(conf, rowType);
    this.bucketIdToFileId = new HashMap<>();
    this.needFixedFileIdSuffix = OptionsResolver.isNonBlockingConcurrencyControl(conf);
  }

  public void write(RowData record) throws IOException {
    try {
      HoodieKey hoodieKey = keyGen.getHoodieKey(record);
      String recordKey = hoodieKey.getRecordKey();
      String partitionPath = hoodieKey.getPartitionPath();

      final int bucketNum = BucketIdentifier.getBucketId(recordKey, indexKeys, numBuckets);
      String bucketId = partitionPath + bucketNum;
      String fileId = bucketIdToFileId.computeIfAbsent(bucketId,
          k -> BucketIdentifier.newBucketFileIdPrefix(bucketNum, needFixedFileIdSuffix));

      if ((lastFileId == null) || !lastFileId.equals(fileId)) {
        LOG.info("Creating new file for partition path " + partitionPath);
        handle = getRowCreateHandle(partitionPath, fileId);
        lastFileId = fileId;
      }
      handle.write(recordKey, partitionPath, record);
    } catch (Throwable throwable) {
      IOException ioException = new IOException("Exception happened during append with bucket index.", throwable);
      LOG.error("Global error thrown while trying to write records in HoodieRowDataCreateHandle", ioException);
      throw ioException;
    }
  }
}
