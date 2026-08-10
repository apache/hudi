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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.config.HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS;

/**
 * A partitioner that globally sorts a {@link Dataset<Row>} based on partition path column and custom columns.
 *
 * @see GlobalSortPartitionerWithRows
 * @see BulkInsertSortMode#GLOBAL_SORT
 */
public class RowCustomColumnsSortPartitioner implements BulkInsertPartitioner<Dataset<Row>> {

  private final String[] sortColumnNames;

  public RowCustomColumnsSortPartitioner(HoodieWriteConfig config) {
    this(getSortColumnName(config), config);
  }

  public RowCustomColumnsSortPartitioner(String[] columnNames, HoodieWriteConfig config) {
    this.sortColumnNames = tryPrependPartitionPathAndSuffixRecordKeyColumns(columnNames, config);
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> records, int outputSparkPartitions) {
    return records
        .sort(Arrays.stream(sortColumnNames).map(Column::new).toArray(Column[]::new))
        .coalesce(outputSparkPartitions);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  /*
   * If possible, we want to sort the data by partition path. Doing so will reduce the number of files written.
   * Suffix the recordKeys as well if BULKINSERT_SUFFIX_RECORD_KEY_FOR_USER_DEFINED_SORT_COLUMNS is enabled, this reduces skew.
   * This will not change the desired sort order, it is just a performance improvement.
   * NOTE: This function is used by BULK_INSERT in row writer mode where Dataset<Row> can be sorted by providing the sort columns.
   **/
  static String[] tryPrependPartitionPathAndSuffixRecordKeyColumns(String[] columnNames, HoodieWriteConfig config) {
    String partitionPath;
    String recordKeyFieldName;
    if (config.populateMetaFields()) {
      partitionPath = HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.getFieldName();
      recordKeyFieldName = HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName();
    } else {
      // TODO: Remove the else block as sorting in row-writer mode is always used with populateMetaFields turned on as it allows easy access to hoodie meta fields
      // without the need for generating them again. Disabling populateMetaFields defeats the purpose of sorting using row-writer and is generally never used.
      // https://issues.apache.org/jira/browse/HUDI-8101
      partitionPath = config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
      recordKeyFieldName = config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key());
    }
    Set<String> sortCols = new LinkedHashSet<>();
    if (!isNullOrEmpty(partitionPath)) {
      sortCols = new LinkedHashSet<>(StringUtils.split(partitionPath, ","));
    }
    sortCols.addAll(Arrays.asList(columnNames));
    boolean suffixRecordKey = config.getBoolean(BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS);
    if (suffixRecordKey) {
      sortCols.add(recordKeyFieldName);
    }
    return sortCols.toArray(new String[0]);
  }

  private static String[] getSortColumnName(HoodieWriteConfig config) {
    return Arrays.stream(config.getUserDefinedBulkInsertPartitionerSortColumns().split(","))
        .map(String::trim).toArray(String[]::new);
  }
}
