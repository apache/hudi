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

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.SortUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.HoodieUTF8StringFactory;

import java.util.Arrays;

import static org.apache.hudi.config.HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS;

/**
 * A partitioner that globally sorts a {@link JavaRDD<HoodieRecord>} based on partition path column and custom columns.
 *
 * @see GlobalSortPartitioner
 * @see BulkInsertSortMode#GLOBAL_SORT
 */
public class RDDCustomColumnsSortPartitioner<T>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  private final String[] sortColumnNames;
  private final SerializableSchema serializableSchema;
  private final boolean consistentLogicalTimestampEnabled;
  private final boolean suffixRecordKey;
  private final HoodieUTF8StringFactory utf8StringFactory =
      SparkAdapterSupport$.MODULE$.sparkAdapter().getUTF8StringFactory();

  public RDDCustomColumnsSortPartitioner(HoodieWriteConfig config) {
    this.serializableSchema = new SerializableSchema(new Schema.Parser().parse(config.getSchema()));
    this.sortColumnNames = getSortColumnName(config);
    this.consistentLogicalTimestampEnabled = config.isConsistentLogicalTimestampEnabled();
    this.suffixRecordKey = config.getBoolean(BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS);
  }

  public RDDCustomColumnsSortPartitioner(String[] columnNames, Schema schema, HoodieWriteConfig config) {
    this.sortColumnNames = columnNames;
    this.serializableSchema = new SerializableSchema(schema);
    this.consistentLogicalTimestampEnabled = config.isConsistentLogicalTimestampEnabled();
    this.suffixRecordKey = config.getBoolean(BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    return records
        .sortBy(record -> SortUtils.getComparableSortColumns(record, sortColumnNames, serializableSchema.get(), suffixRecordKey, consistentLogicalTimestampEnabled,
            utf8StringFactory::wrapArrayOfObjects), true, outputSparkPartitions);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  private String[] getSortColumnName(HoodieWriteConfig config) {
    return Arrays.stream(config.getUserDefinedBulkInsertPartitionerSortColumns().split(","))
        .map(String::trim).toArray(String[]::new);
  }
}
