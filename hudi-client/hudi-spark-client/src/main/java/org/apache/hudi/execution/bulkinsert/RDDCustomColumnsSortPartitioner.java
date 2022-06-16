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
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;

/**
 * A partitioner that does local sorting for each RDD partition based on the tuple of
 * values of the columns configured for ordering.
 *
 * @param <T> HoodieRecordPayload type
 */
public class RDDCustomColumnsSortPartitioner<T extends HoodieRecordPayload>
    extends RepartitioningBulkInsertPartitionerBase<JavaRDD<HoodieRecord<T>>> {

  private final String[] orderByColumnNames;
  private final SerializableSchema serializableSchema;
  private final boolean consistentLogicalTimestampEnabled;

  public RDDCustomColumnsSortPartitioner(HoodieTableConfig tableConfig, HoodieWriteConfig config) {
    super(tableConfig);
    this.serializableSchema = new SerializableSchema(new Schema.Parser().parse(config.getSchema()));
    this.orderByColumnNames = getOrderByColumnNames(config);
    this.consistentLogicalTimestampEnabled = config.isConsistentLogicalTimestampEnabled();
  }

  public RDDCustomColumnsSortPartitioner(String[] columnNames,
                                         Schema schema,
                                         boolean consistentLogicalTimestampEnabled,
                                         HoodieTableConfig tableConfig) {
    super(tableConfig);
    this.orderByColumnNames = columnNames;
    this.serializableSchema = new SerializableSchema(schema);
    this.consistentLogicalTimestampEnabled = consistentLogicalTimestampEnabled;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
                                                     int outputSparkPartitions) {
    final String[] sortColumns = this.orderByColumnNames;
    final SerializableSchema schema = this.serializableSchema;
    final boolean consistentLogicalTimestampEnabled = this.consistentLogicalTimestampEnabled;

    // NOTE: In case of partitioned table even "global" ordering (across all RDD partitions) could
    //       not change table's partitioning and therefore there's no point in doing global sorting
    //       across "physical" partitions, and instead we can reduce total amount of data being
    //       shuffled by doing do "local" sorting:
    //          - First, re-partitioning dataset such that "logical" partitions are aligned w/
    //          "physical" ones
    //          - Sorting locally w/in RDD ("logical") partitions
    //
    //       Non-partitioned tables will be globally sorted.
    if (isPartitionedTable) {
      Comparator<Pair<String, String>> sortingKeyComparator =
          Comparator.comparing((Function<Pair<String, String>, String> & Serializable) Pair::getValue);

      PartitionPathRDDPartitioner partitioner =
          new PartitionPathRDDPartitioner((pair) -> ((Pair<String, String>) pair).getKey(), outputSparkPartitions);

      // Both partition-path and record-key are extracted, since
      //    - Partition-path will be used for re-partitioning (as called out above)
      //    - Record-key will be used for sorting the records w/in individual partitions
      return records.mapToPair(record -> {
        String sortingKey = getSortingKey(record, sortColumns, schema, consistentLogicalTimestampEnabled);
        String partitionPath = record.getPartitionPath();
        return new Tuple2<>(Pair.of(partitionPath, sortingKey), record);
      })
          .repartitionAndSortWithinPartitions(partitioner, sortingKeyComparator)
          .values();
    } else {
      return records.sortBy(record -> getSortingKey(record, sortColumns, schema, consistentLogicalTimestampEnabled),
          true, outputSparkPartitions);
    }
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  private static String[] getOrderByColumnNames(HoodieWriteConfig config) {
    return Arrays.stream(config.getUserDefinedBulkInsertPartitionerSortColumns().split(","))
        .map(String::trim).toArray(String[]::new);
  }

  private static String getSortingKey(HoodieRecord<? extends HoodieRecordPayload> record,
                                      String[] sortColumns,
                                      SerializableSchema schema,
                                      boolean consistentLogicalTimestampEnabled) {
    Object columnValues = HoodieAvroUtils.getRecordColumnValues(record, sortColumns, schema.get(), consistentLogicalTimestampEnabled);
    // null values are replaced with empty string for null_first order
    if (columnValues == null) {
      return StringUtils.EMPTY_STRING;
    } else {
      return StringUtils.objToString(columnValues);
    }
  }
}
