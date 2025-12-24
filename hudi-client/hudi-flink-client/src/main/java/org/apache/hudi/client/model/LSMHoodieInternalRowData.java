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
 * RowData implementation for Hoodie Row. It wraps an {@link RowData} and keeps meta columns locally. But the {@link RowData}
 * does include the meta columns as well just that {@link LSMHoodieInternalRowData} will intercept queries for meta columns and serve from its
 * copy rather than fetching from {@link RowData}.
 *
 * <p>The wrapped {@link RowData} does not contain hoodie metadata fields.
 */
public class LSMHoodieInternalRowData extends AbstractHoodieRowData {
  private final StringData[] lsmInternalMetaColumns;

  public static int LSM_INTERNAL_RECORD_KEY_META_FIELD_ORD = 0;
  public static int LSM_INTERNAL_COMMIT_SEQNO_META_FIELD_ORD = 1;

  public LSMHoodieInternalRowData(StringData recordKey, StringData seqId, RowData row) {
    super(row, 2, false);
    lsmInternalMetaColumns = new StringData[metaColumnsNum];
    lsmInternalMetaColumns[0] = recordKey;
    lsmInternalMetaColumns[1] = seqId;
  }

  @Override
  public int getArity() {
    return metaColumnsNum + row.getArity();
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (ordinal < metaColumnsNum) {
      return null == getLsmInternalMetaColumnVal(ordinal);
    }
    return row.isNullAt(rebaseOrdinal(ordinal));
  }

  @Override
  public StringData getString(int ordinal) {
    if (ordinal < metaColumnsNum) {
      return getLsmInternalMetaColumnVal(ordinal);
    }
    return row.getString(rebaseOrdinal(ordinal));
  }

  protected StringData getLsmInternalMetaColumnVal(int ordinal) {
    return this.lsmInternalMetaColumns[ordinal];
  }

  @Override
  protected int rebaseOrdinal(int ordinal) {
    return ordinal - metaColumnsNum;
  }
}
