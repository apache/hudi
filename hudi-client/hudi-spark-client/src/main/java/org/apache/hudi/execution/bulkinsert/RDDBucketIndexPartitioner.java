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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.FlatLists;
import org.apache.hudi.table.BucketIndexBulkInsertPartitioner;

import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Abstract of bucket index bulk_insert partitioner
 */

public abstract class RDDBucketIndexPartitioner<T> extends BucketIndexBulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

  public static final Logger LOG = LogManager.getLogger(RDDBucketIndexPartitioner.class);

  public RDDBucketIndexPartitioner(HoodieTable table, String sortString, boolean preserveHoodieMetadata) {
    super(table, sortString, preserveHoodieMetadata);
  }

  /**
   * Execute partition using the given partitioner.
   * If sorting is required, will do it within each data partition:
   * - if sortColumnNames is specified, apply sort to the column (the behaviour is the same as `RDDCustomColumnsSortPartitioner`)
   * - if table requires sort or BulkInsertSortMode is not None, then sort by record key within partition.
   * By default, do partition only.
   *
   * @param records
   * @param partitioner a default partition that accepts `HoodieKey` as the partition key
   * @return
   */

  public JavaRDD<HoodieRecord<T>> doPartition(JavaRDD<HoodieRecord<T>> records, Partitioner partitioner) {
    if (sortColumnNames != null && sortColumnNames.length > 0) {
      return doPartitionAndCustomColumnSort(records, partitioner);
    } else if (table.requireSortedRecords() || table.getConfig().getBulkInsertSortMode() != BulkInsertSortMode.NONE) {
      return doPartitionAndSortByRecordKey(records, partitioner);
    } else {
      // By default, do partition only
      return records.mapToPair(record -> new Tuple2<>(record.getKey(), record))
          .partitionBy(partitioner).map(Tuple2::_2);
    }
  }

  /**
   * Sort by specified column value. The behaviour is the same as `RDDCustomColumnsSortPartitioner`
   *
   * @param records
   * @param partitioner
   * @return
   */
  private JavaRDD<HoodieRecord<T>> doPartitionAndCustomColumnSort(JavaRDD<HoodieRecord<T>> records, Partitioner partitioner) {
    final String[] sortColumns = sortColumnNames;
    final SerializableSchema schema = new SerializableSchema(HoodieAvroUtils.addMetadataFields((new Schema.Parser().parse(table.getConfig().getSchema()))));
    Comparator<HoodieRecord<T>> comparator = (Comparator<HoodieRecord<T>> & Serializable) (t1, t2) -> {
      FlatLists.ComparableList obj1 = FlatLists.ofComparableArray(t1.getColumnValues(schema.get(), sortColumns, consistentLogicalTimestampEnabled));
      FlatLists.ComparableList obj2 = FlatLists.ofComparableArray(t2.getColumnValues(schema.get(), sortColumns, consistentLogicalTimestampEnabled));
      return obj1.compareTo(obj2);
    };

    return records.mapToPair(record -> new Tuple2<>(record, record))
        .repartitionAndSortWithinPartitions(new Partitioner() {
          @Override
          public int numPartitions() {
            return partitioner.numPartitions();
          }

          @Override
          public int getPartition(Object key) {
            return partitioner.getPartition(((HoodieRecord) key).getKey());
          }
        }, comparator).map(Tuple2::_2);
  }

  /**
   * Sort by record key within each partition. The behaviour is the same as BulkInsertSortMode.PARTITION_SORT.
   *
   * @param records
   * @param partitioner
   * @return
   */
  private JavaRDD<HoodieRecord<T>> doPartitionAndSortByRecordKey(JavaRDD<HoodieRecord<T>> records, Partitioner partitioner) {
    if (table.getConfig().getBulkInsertSortMode() == BulkInsertSortMode.GLOBAL_SORT) {
      LOG.warn("Bucket index does not support global sort mode, the sort will only be done within each data partition");
    }

    Comparator<HoodieKey> comparator = (Comparator<HoodieKey> & Serializable) (t1, t2) -> t1.getRecordKey().compareTo(t2.getRecordKey());

    return records.mapToPair(record -> new Tuple2<>(record.getKey(), record))
        .repartitionAndSortWithinPartitions(partitioner, comparator)
        .map(Tuple2::_2);
  }
}
