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

package org.apache.hudi.client.model;

import org.apache.flink.table.data.RowData;

/**
 * The factory clazz for hoodie row data.
 */
public abstract class HoodieRowDataCreation {
  /**
   * Creates a {@link AbstractHoodieRowData} instance based on the given configuration.
   */
  public static AbstractHoodieRowData create(
      String commitTime,
      String commitSeqNumber,
      String recordKey,
      String partitionPath,
      String fileName,
      RowData row,
      boolean withOperation,
      boolean withMetaFields) {
    return withMetaFields
        ? new HoodieRowDataWithMetaFields(commitTime, commitSeqNumber, recordKey, partitionPath, fileName, row, withOperation)
        : new HoodieRowData(commitTime, commitSeqNumber, recordKey, partitionPath, fileName, row, withOperation);
  }
}
