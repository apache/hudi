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

import org.apache.hudi.common.model.HoodieOperation;

/**
 * Callback interface for handling updates to the base file of the file group.
 */
public interface BaseFileUpdateCallback<T> {
  /**
   * Callback method to handle updates to a record already present in the base file.
   * @param recordKey the key of the record being updated
   * @param previousRecord the record in the base file before the update
   * @param mergedRecord the result of merging the previous and new records
   */
  void onUpdate(String recordKey, BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord);

  /**
   * Callback method to handle insertion of a new record into the base file.
   * @param recordKey the key of the record being inserted
   * @param newRecord the new record being added to the base file
   */
  void onInsert(String recordKey, BufferedRecord<T> newRecord);

  /**
   * Callback method to handle deletion of a record from the base file.
   * @param recordKey the key of the record being deleted
   * @param previousRecord the record in the base file before deletion
   * @param hoodieOperation the operation type of the incoming record, used to infer type of delete operation
   */
  void onDelete(String recordKey, BufferedRecord<T> previousRecord, HoodieOperation hoodieOperation);

  /**
   * Used for write failure retraction.
   */
  void onFailure(String recordKey);
}
