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
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.model.HoodieRecord.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_SEQNO_METADATA_FIELD_ORD;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD_ORD;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_META_FIELD_ORD;
import static org.apache.hudi.common.model.HoodieRecord.OPERATION_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_META_FIELD_ORD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_META_FIELD_ORD;

/**
 * RowData implementation for Hoodie Row with meta data columns
 *
 * <p>The wrapped {@link RowData} only contain two metadata fields (recordkey and sequenceId).
 */
public class LSMHoodieRowData extends AbstractHoodieRowData {

  private final StringData[] lsmMetaColumns;
  private final boolean withOperation;

  private static final List<String> LSM_HOODIE_META_COLUMNS =
      CollectionUtils.createImmutableList(COMMIT_TIME_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD,
          FILENAME_METADATA_FIELD, RECORD_KEY_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD);

  private static final List<String> LSM_HOODIE_META_COLUMNS_WITH_OPERATION =
      CollectionUtils.createImmutableList(COMMIT_TIME_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD,
          FILENAME_METADATA_FIELD, OPERATION_METADATA_FIELD, RECORD_KEY_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD);

  private static final Map<String, Integer> LSM_HOODIE_META_COLUMNS_NAME_TO_POS =
      IntStream.range(0, LSM_HOODIE_META_COLUMNS.size())
          .mapToObj(idx -> Pair.of(LSM_HOODIE_META_COLUMNS.get(idx), idx))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

  private static final Map<Integer, Integer> INTERNAL_METADATA_FIELD_ORD_MAP = CollectionUtils.createImmutableMap(
      Pair.of(COMMIT_TIME_METADATA_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_TO_POS.get(COMMIT_TIME_METADATA_FIELD)),
      Pair.of(COMMIT_SEQNO_METADATA_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_TO_POS.get(COMMIT_SEQNO_METADATA_FIELD)),
      Pair.of(RECORD_KEY_META_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_TO_POS.get(RECORD_KEY_METADATA_FIELD)),
      Pair.of(PARTITION_PATH_META_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_TO_POS.get(PARTITION_PATH_METADATA_FIELD)),
      Pair.of(FILENAME_META_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_TO_POS.get(FILENAME_METADATA_FIELD))
  );

  private static final Map<String, Integer> LSM_HOODIE_META_COLUMNS_NAME_WITH_OPERATION_TO_POS =
      IntStream.range(0, LSM_HOODIE_META_COLUMNS_WITH_OPERATION.size())
          .mapToObj(idx -> Pair.of(LSM_HOODIE_META_COLUMNS_WITH_OPERATION.get(idx), idx))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

  private static final int OPERATION_METADATA_FIELD_ORD = 5;

  private static final Map<Integer, Integer> INTERNAL_METADATA_FIELD_WITH_OPERATION_ORD_MAP = CollectionUtils.createImmutableMap(
      Pair.of(COMMIT_TIME_METADATA_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_WITH_OPERATION_TO_POS.get(COMMIT_TIME_METADATA_FIELD)),
      Pair.of(COMMIT_SEQNO_METADATA_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_WITH_OPERATION_TO_POS.get(COMMIT_SEQNO_METADATA_FIELD)),
      Pair.of(RECORD_KEY_META_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_WITH_OPERATION_TO_POS.get(RECORD_KEY_METADATA_FIELD)),
      Pair.of(PARTITION_PATH_META_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_WITH_OPERATION_TO_POS.get(PARTITION_PATH_METADATA_FIELD)),
      Pair.of(FILENAME_META_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_WITH_OPERATION_TO_POS.get(FILENAME_METADATA_FIELD)),
      Pair.of(OPERATION_METADATA_FIELD_ORD, LSM_HOODIE_META_COLUMNS_NAME_WITH_OPERATION_TO_POS.get(OPERATION_METADATA_FIELD))
  );

  public LSMHoodieRowData(StringData commitTime,
                          StringData partitionPath,
                          StringData fileName,
                          RowData row,
                          boolean withOperation) {
    super(row, 3, withOperation);
    this.withOperation = withOperation;
    this.lsmMetaColumns = new StringData[metaColumnsNum];
    lsmMetaColumns[0] = commitTime;
    lsmMetaColumns[1] = partitionPath;
    lsmMetaColumns[2] = fileName;
    if (withOperation) {
      lsmMetaColumns[3] = StringData.fromString(HoodieOperation.fromValue(row.getRowKind().toByteValue()).getName());
    }
  }

  @Override
  public int getArity() {
    return metaColumnsNum + row.getArity();
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (ordinal < metaColumnsNum) {
      return null == getLsmMetaColumnVal(ordinal);
    }
    return row.isNullAt(rebaseOrdinal(ordinal));
  }

  @Override
  public StringData getString(int ordinal) {
    if (ordinal < metaColumnsNum + 2) {
      int realPos = withOperation ? INTERNAL_METADATA_FIELD_WITH_OPERATION_ORD_MAP.get(ordinal) : INTERNAL_METADATA_FIELD_ORD_MAP.get(ordinal);
      if (realPos < metaColumnsNum) {
        return getLsmMetaColumnVal(realPos);
      }
      return row.getString(rebaseOrdinal(realPos));
    }
    return row.getString(rebaseOrdinal(ordinal));
  }

  protected StringData getLsmMetaColumnVal(int ordinal) {
    return this.lsmMetaColumns[ordinal];
  }

  @Override
  protected int rebaseOrdinal(int ordinal) {
    return ordinal - metaColumnsNum;
  }
}
