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
import org.apache.flink.table.data.StringData;

/**
 * RowData implementation for Hoodie Row. It wraps an {@link RowData} and keeps meta columns locally,
 * but the meta columns array only contains one updated meta field, e.g., updated `FILENAME_METADATA_FIELD`
 * for base file writing during compaction.
 */
public class HoodieRowDataWithUpdatedMetaField extends HoodieRowDataWithMetaFields {
  private final int updatedMetaOrdinal;

  public HoodieRowDataWithUpdatedMetaField(
      String[] metaVals,
      int updatedMetaOrdinal,
      RowData row,
      boolean withOperation) {
    super(metaVals[0], metaVals[1], metaVals[2], metaVals[3], metaVals[4], row, withOperation);
    this.updatedMetaOrdinal = updatedMetaOrdinal;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (updatedMetaOrdinal == ordinal) {
      return null == getMetaColumnVal(ordinal);
    } else {
      return row.isNullAt(rebaseOrdinal(ordinal));
    }
  }

  @Override
  public StringData getString(int ordinal) {
    if (updatedMetaOrdinal == ordinal) {
      return StringData.fromString(getMetaColumnVal(ordinal));
    }
    return row.getString(rebaseOrdinal(ordinal));
  }
}
