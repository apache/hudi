/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.index.bloom;

import java.io.Serializable;
import java.util.Set;

/**
 * IndexFile filter to assist in look up of a record key.
 */
public interface IndexFileFilter extends Serializable {

  /**
   * Fetches all matching files for a given record key and partition.
   *
   * @param partitionPath the partition path of interest
   * @param recordKey the record key to be looked up
   * @return the {@link Set} of matching file names where the record could potentially be present.
   */
  Set<String> getMatchingFiles(String partitionPath, String recordKey);

}
