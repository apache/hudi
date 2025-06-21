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

import java.io.Serializable;

/**
 * Some SparkHoodiePartitioner classes can store a lot of data. To prevent
 * unnecessary serialization and transmission of data, SparkBucketInfoGetter
 * was created to store the minimal data needed for getBucketInfo.
 * getBucketInfo is invoked on the executor
 */
public interface SparkBucketInfoGetter extends Serializable {

  /**
   * @param bucketNumber spark partition id
   * @return bucket info for that spark partition
   */
  BucketInfo getBucketInfo(int bucketNumber);

}
