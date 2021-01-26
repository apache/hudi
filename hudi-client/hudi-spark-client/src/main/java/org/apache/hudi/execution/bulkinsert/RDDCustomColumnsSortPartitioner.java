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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

/**
 * A partitioner that does sorting based on specified column values for each RDD partition.
 *
 * @param <T> HoodieRecordPayload type
 */
public class RDDCustomColumnsSortPartitioner<T extends HoodieRecordPayload>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  private final String[] sortColumnNames;
  private final SerializableSchema serializableSchema;

  public RDDCustomColumnsSortPartitioner(String[] columnNames, Schema schema) {
    this.sortColumnNames = columnNames;
    this.serializableSchema = new SerializableSchema(schema);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    final String[] sortColumns = this.sortColumnNames;
    final SerializableSchema schema = this.serializableSchema;
    return records.sortBy(record -> getRecordSortColumnValues(record, sortColumns, schema), 
        true, outputSparkPartitions);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  private static Object getRecordSortColumnValues(HoodieRecord<? extends HoodieRecordPayload> record,
                                                  String[] sortColumns,
                                                  SerializableSchema schema) {
    try {
      GenericRecord genericRecord = (GenericRecord) record.getData().getInsertValue(schema.get()).get();
      if (sortColumns.length == 1) {
        return HoodieAvroUtils.getNestedFieldVal(genericRecord, sortColumns[0], true);
      } else {
        StringBuilder sb = new StringBuilder();
        for (String col : sortColumns) {
          sb.append(HoodieAvroUtils.getNestedFieldValAsString(genericRecord, col, true));
        }

        return sb.toString();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read record with key:" + record.getKey(), e);
    }
  }
}
