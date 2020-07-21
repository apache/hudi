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
import org.apache.hudi.common.model.HoodieRecordPayload;

import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

public class RDDPartitionRangePartitioner<T extends HoodieRecordPayload>
    extends BulkInsertInternalPartitioner<T> implements Serializable {
  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records,
      int outputSparkPartitions) {
    JavaPairRDD<String, HoodieRecord<T>> pairRDD = records.mapToPair(record ->
        new Tuple2(
            new StringBuilder()
                .append(record.getPartitionPath())
                .append("+")
                .append(record.getRecordKey())
                .toString(), record));
    Ordering<String> ordering = Ordering$.MODULE$
        .comparatorToOrdering(Comparator.<String>naturalOrder());
    ClassTag<String> classTag = ClassTag$.MODULE$.apply(String.class);
    return pairRDD.partitionBy(new RangePartitioner<String, HoodieRecord<T>>(
        outputSparkPartitions, pairRDD.rdd(), true,
        ordering, classTag)).map(pair -> pair._2);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return false;
  }
}
