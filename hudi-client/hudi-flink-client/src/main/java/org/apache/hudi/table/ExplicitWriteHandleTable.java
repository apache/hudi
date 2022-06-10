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

package org.apache.hudi.table;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;

/**
 * HoodieTable that need to pass in the
 * {@link org.apache.hudi.io.HoodieWriteHandle} explicitly.
 */
public interface ExplicitWriteHandleTable<T> {
  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context     HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant Time for the action
   * @param records     hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> upsert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> records);

  /**
   * Insert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context     HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant Time for the action
   * @param records     hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> insert(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> records);

  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied instantTime {@link HoodieKey}s will be
   * de-duped and non existent keys will be removed before deleting.
   *
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context     HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant Time for the action
   * @param keys   {@link List} of {@link HoodieKey}s to be deleted
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> delete(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieKey> keys);

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   *
   * <p>This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param preppedRecords  hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> preppedRecords);

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   *
   * <p>This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * <p>Specifies the write handle explicitly in order to have fine grained control with
   * the underneath file.
   *
   * @param context    HoodieEngineContext
   * @param instantTime Instant Time for the action
   * @param preppedRecords  hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> insertPrepped(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> preppedRecords);

  /**
   * Replaces all the existing records and inserts the specified new records into Hoodie table at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param context HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant time for the replace action
   * @param records input records
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> insertOverwrite(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> records);

  /**
   * Deletes all the existing records of the Hoodie table and inserts the specified new records into Hoodie table at the supplied instantTime,
   * for the partition paths contained in input records.
   *
   * @param context HoodieEngineContext
   * @param writeHandle The write handle
   * @param instantTime Instant time for the replace action
   * @param records input records
   * @return HoodieWriteMetadata
   */
  HoodieWriteMetadata<List<WriteStatus>> insertOverwriteTable(
      HoodieEngineContext context,
      HoodieWriteHandle<?, ?, ?, ?> writeHandle,
      String instantTime,
      List<HoodieRecord<T>> records);
}
