/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieLegacyAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A partitioner that does sorting based on specified column values for Java client.
 *
 * @param <T> HoodieRecordPayload type
 */
public class JavaCustomColumnsSortPartitioner<T>
    implements BulkInsertPartitioner<List<HoodieRecord<T>>> {

  private final String[] sortColumnNames;
  private final Schema schema;
  private final boolean consistentLogicalTimestampEnabled;

  public JavaCustomColumnsSortPartitioner(String[] columnNames, Schema schema, boolean consistentLogicalTimestampEnabled) {
    this.sortColumnNames = columnNames;
    this.schema = schema;
    this.consistentLogicalTimestampEnabled = consistentLogicalTimestampEnabled;
  }

  @Override
  public List<HoodieRecord<T>> repartitionRecords(
      List<HoodieRecord<T>> records, int outputPartitions) {
    return records.stream().sorted((o1, o2) -> {
      Object values1 = HoodieAvroUtils.getRecordColumnValues((HoodieLegacyAvroRecord)o1, sortColumnNames, schema, consistentLogicalTimestampEnabled);
      Object values2 = HoodieAvroUtils.getRecordColumnValues((HoodieLegacyAvroRecord)o2, sortColumnNames, schema, consistentLogicalTimestampEnabled);
      return values1.toString().compareTo(values2.toString());
    }).collect(Collectors.toList());
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }
}
