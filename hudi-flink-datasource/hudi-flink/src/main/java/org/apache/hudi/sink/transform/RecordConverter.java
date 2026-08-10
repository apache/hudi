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

package org.apache.hudi.sink.transform;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.table.action.commit.BucketInfo;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * Function that converts the given {@link RowData} into a hoodie record.
 */
public interface RecordConverter extends Serializable {
  HoodieRecord convert(RowData dataRow, BucketInfo bucketInfo);

  static RecordConverter getInstance(RowDataKeyGen keyGen) {
    return (dataRow, bucketInfo) -> {
      String key = keyGen.getRecordKey(dataRow);
      HoodieOperation operation = HoodieOperation.fromValue(dataRow.getRowKind().toByteValue());
      HoodieKey hoodieKey = new HoodieKey(key, bucketInfo.getPartitionPath());
      return new HoodieFlinkRecord(hoodieKey, operation, dataRow);
    };
  }
}
