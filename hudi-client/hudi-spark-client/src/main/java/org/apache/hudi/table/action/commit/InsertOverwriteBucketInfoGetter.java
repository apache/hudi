/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.fs.FSUtils;

import java.util.Map;

/**
 * Companion class to SparkInsertOverwritePartitioner
 */
public class InsertOverwriteBucketInfoGetter extends MapBasedSparkBucketInfoGetter {
  public InsertOverwriteBucketInfoGetter(Map<Integer, BucketInfo> bucketInfoMap) {
    super(bucketInfoMap);
  }

  @Override
  public BucketInfo getBucketInfo(int bucketNumber) {
    BucketInfo bucketInfo = super.getBucketInfo(bucketNumber);
    switch (bucketInfo.bucketType) {
      case INSERT:
        return bucketInfo;
      case UPDATE:
        // Insert overwrite always generates new bucket file id
        return new BucketInfo(BucketType.INSERT, FSUtils.createNewFileIdPfx(), bucketInfo.partitionPath);
      default:
        throw new AssertionError();
    }
  }
}
