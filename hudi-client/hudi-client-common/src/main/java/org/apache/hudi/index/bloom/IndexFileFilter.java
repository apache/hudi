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

package org.apache.hudi.index.bloom;

import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.Set;

/**
 * IndexFile filter to assist in look up of a record key.
 */
public interface IndexFileFilter extends Serializable {

  /**
   * Fetches all matching files and partition pair for a given record key and partition path.
   *
   * @param partitionPath the partition path of interest
   * @param recordKey     the record key to be looked up
   * @return the {@link Set} of matching <Partition path, file name> pairs where the record could potentially be
   * present.
   */
  Set<Pair<String, String>> getMatchingFilesAndPartition(String partitionPath, String recordKey);

}
