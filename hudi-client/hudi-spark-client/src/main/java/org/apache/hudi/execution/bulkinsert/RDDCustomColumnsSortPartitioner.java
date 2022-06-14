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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;

/**
 * A partitioner that does sorting based on specified column values for each RDD partition.
 *
 * @param <T> HoodieRecordPayload type
 */
public class RDDCustomColumnsSortPartitioner<T>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  private final String[] sortColumnNames;
  private final SerializableSchema serializableSchema;
  private final boolean consistentLogicalTimestampEnabled;

  public RDDCustomColumnsSortPartitioner(HoodieWriteConfig config) {
    this.serializableSchema = new SerializableSchema(new Schema.Parser().parse(config.getSchema()));
    this.sortColumnNames = getSortColumnName(config);
    this.consistentLogicalTimestampEnabled = config.isConsistentLogicalTimestampEnabled();
  }

  public RDDCustomColumnsSortPartitioner(String[] columnNames, Schema schema, boolean consistentLogicalTimestampEnabled) {
    this.sortColumnNames = columnNames;
    this.serializableSchema = new SerializableSchema(schema);
    this.consistentLogicalTimestampEnabled = consistentLogicalTimestampEnabled;
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    final String[] sortColumns = this.sortColumnNames;
    final SerializableSchema schema = this.serializableSchema;
    final boolean consistentLogicalTimestampEnabled = this.consistentLogicalTimestampEnabled;
    return records.sortBy(
        record -> {
          Object recordValue = HoodieAvroUtils.getRecordColumnValues((HoodieAvroRecord)record, sortColumns, schema, consistentLogicalTimestampEnabled);
          // null values are replaced with empty string for null_first order
          if (recordValue == null) {
            return StringUtils.EMPTY_STRING;
          } else {
            return StringUtils.objToString(recordValue);
          }
        },
        true, outputSparkPartitions);
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
