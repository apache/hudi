/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.adapter;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Adapter clazz for {@link SupportsRowLevelUpdate}.
 */
public interface SupportsRowLevelUpdateAdapter extends SupportsRowLevelUpdate {
  @Override
  default RowLevelUpdateInfo applyRowLevelUpdate(List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context) {
    return applyRowLevelUpdate(updatedColumns);
  }

  RowLevelUpdateInfoAdapter applyRowLevelUpdate(List<Column> updatedColumns);

  /**
   * Adapter clazz for {@link RowLevelUpdateInfo}.
   */
  interface RowLevelUpdateInfoAdapter extends RowLevelUpdateInfo {
  }
}
