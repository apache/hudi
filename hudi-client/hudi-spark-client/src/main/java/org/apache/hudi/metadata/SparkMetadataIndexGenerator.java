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

package org.apache.hudi.metadata;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.data.HoodieJavaRDD;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import static org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter.mapPartitionKeyToSparkPartition;

public class SparkMetadataIndexGenerator extends MetadataIndexGenerator {

  @Override
  protected HoodieData<WriteStatus> repartitionRecordsByHudiPartition(HoodieData<WriteStatus> records, int numPartitions) {
    JavaRDD<WriteStatus> writeStatusJavaRDD = HoodieJavaRDD.getJavaRDD(records);
    return HoodieJavaRDD.of(writeStatusJavaRDD.mapToPair(new PairFunction<WriteStatus, String, WriteStatus>() {
      @Override
      public Tuple2<String, WriteStatus> call(WriteStatus writeStatus) throws Exception {
        return new Tuple2<>(writeStatus.getPartitionPath(), writeStatus);
      }
    }).partitionBy(new Partitioner() {
      @Override
      public int numPartitions() {
        return numPartitions;
      }

      @Override
      public int getPartition(Object key) {
        return mapPartitionKeyToSparkPartition((String) key, numPartitions);
      }
    }).values());
  }
}
