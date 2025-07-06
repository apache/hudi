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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.model.HoodieRecord;

/**
 * Callback interface for handling updates to the base file of the file group.
 * @param <T> Type of the record payload.
 */
public interface FileGroupUpdateCallback<T> {
  /**
   * Callback method to handle updates to a record already present in the base file.
   * @param previousRecord the record in the base file before the update
   * @param newRecord the incoming record
   * @param mergedRecord the result of merging the previous and new records
   */
  void onUpdate(HoodieRecord<T> previousRecord, HoodieRecord<T> newRecord, HoodieRecord<T> mergedRecord);

  /**
   * Callback method to handle insertion of a new record into the base file.
   * @param newRecord the new record being added to the base file
   */
  void onInsert(HoodieRecord<T> newRecord);

  /**
   * Callback method to handle deletion of a record from the base file.
   * @param previousRecord the record in the base file before deletion
   * @param inputRecord the record that triggered the deletion
   */
  void onDelete(HoodieRecord<T> previousRecord, HoodieRecord<T> inputRecord);
}
