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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.table.action.commit.BucketInfo;

import java.util.Iterator;
import java.util.List;

/**
 * Interface for flink write client that supports writing RowData directly into the underneath filesystem.
 */
public interface FlinkRowDataWriteClient<T> {

  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * @param records rowdatas needed to be written, in form of iterator
   * @param bucketInfo the bucket info for the target bucket
   * @param instantTime instant time to commit the data
   * @return Collection of WriteStatus to inspect errors and counts
   */
  List<WriteStatus> upsert(Iterator<HoodieRecord<T>> records, BucketInfo bucketInfo, String instantTime);

  /**
   * Insert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * @param records rowdatas needed to be written, in form of iterator
   * @param bucketInfo the bucket info for the target bucket
   * @param instantTime instant time to commit the data
   * @return Collection of WriteStatus to inspect errors and counts
   */
  List<WriteStatus> insert(Iterator<HoodieRecord<T>> records, BucketInfo bucketInfo, String instantTime);

  /**
   * Replaces all the existing records and inserts the specified new records at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param records rowdatas needed to be written, in form of iterator
   * @param bucketInfo the bucket info for the target bucket
   * @param instantTime instant time to commit the data
   * @return Collection of WriteStatus to inspect errors and counts
   */
  List<WriteStatus> insertOverwrite(Iterator<HoodieRecord<T>> records, BucketInfo bucketInfo, String instantTime);

  /**
   * Deletes all the existing records of the Hoodie table and inserts the specified new records into Hoodie table at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param records rowdatas needed to be written, in form of iterator
   * @param bucketInfo the bucket info for the target bucket
   * @param instantTime instant time to commit the data
   * @return Collection of WriteStatus to inspect errors and counts
   */
  List<WriteStatus> insertOverwriteTable(Iterator<HoodieRecord<T>> records, BucketInfo bucketInfo, String instantTime);
}
