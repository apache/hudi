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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;

/**
 * A partitioner that does sorting based on specified column values for each RDD partition.
 *
 * @param <T> HoodieRecordPayload type
 */
public class RDDCustomColumnsSortPartitioner<T extends HoodieRecordPayload>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  private final String[] sortColumnNames;
  private final SerializableSchema serializableSchema;

  public RDDCustomColumnsSortPartitioner(HoodieWriteConfig config) {
    this.serializableSchema = new SerializableSchema(new Schema.Parser().parse(config.getSchema()));
    this.sortColumnNames = getSortColumnName(config);
  }

  public RDDCustomColumnsSortPartitioner(String[] columnNames, Schema schema) {
    this.sortColumnNames = columnNames;
    this.serializableSchema = new SerializableSchema(schema);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    final String[] sortColumns = this.sortColumnNames;
    final SerializableSchema schema = this.serializableSchema;
    return records.sortBy(
        record -> HoodieAvroUtils.getRecordColumnValues(record, sortColumns, schema),
        true, outputSparkPartitions);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  private String[] getSortColumnName(HoodieWriteConfig config) {
    return config.getUserDefinedBulkInsertPartitionerSortColumns().split(",");
  }
}
