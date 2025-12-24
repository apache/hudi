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

package org.apache.hudi.io.lsm;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import java.util.Iterator;
import java.util.List;

public interface RecordMergeWrapper<T> {

  /**
   * merge a group of HoodieRecords with the same record key
   * @param recordGroup
   * @return
   */
  Option<T> merge(List<HoodieRecord> recordGroup);

  /**
   * merge a iterator of HoodieRecords with the same record key
   * @param sameKeyIterator
   * @return
   */
  Option<T> merge(Iterator<HoodieRecord> sameKeyIterator);

  /**
   * Sequentially merge HoodieRecords with the same record key
   * @param record
   */
  void merge(HoodieRecord record);

  /**
   * Obtain the sequentially merged results of HoodieRecords
   * @return Option<InternalRow>
   */
  Option<T> getMergedResult();

  void reset();
}
