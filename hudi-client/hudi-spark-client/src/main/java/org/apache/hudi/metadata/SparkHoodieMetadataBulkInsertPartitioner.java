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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

/**
 * A {@code BulkInsertPartitioner} implementation for Metadata Table to improve performance of initialization of metadata
 * table partition when a very large number of records are inserted.
 *
 * This partitioner requires the records to tbe already tagged with the appropriate file slice.
 */
public class SparkHoodieMetadataBulkInsertPartitioner implements BulkInsertPartitioner<JavaRDD<HoodieRecord>>, Serializable {

  private class FileGroupPartitioner extends Partitioner {
    private int numFileGroups;

    public FileGroupPartitioner(int numFileGroups) {
      this.numFileGroups = numFileGroups;
    }

    @Override
    public int getPartition(Object key) {
      return ((Tuple2<Integer, String>)key)._1;
    }

    @Override
    public int numPartitions() {
      return numFileGroups;
    }
  }

  // The file group count in the partition
  private int fileGroupCount;
  // FileIDs for the various partitions
  private List<String> fileIDPfxs;

  public SparkHoodieMetadataBulkInsertPartitioner(int fileGroupCount) {
    this.fileGroupCount = fileGroupCount;
  }

  @Override
  public JavaRDD<HoodieRecord> repartitionRecords(JavaRDD<HoodieRecord> records, int outputSparkPartitions) {
    Comparator<Tuple2<Integer, String>> keyComparator = (Comparator<Tuple2<Integer, String>> & Serializable)(t1, t2) -> {
      return t1._2.compareTo(t2._2);
    };

    // Partition the records by their location
    JavaRDD<HoodieRecord> partitionedRDD = records
        .keyBy(r -> new Tuple2<Integer, String>(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(r.getRecordKey(), fileGroupCount), r.getRecordKey()))
        .repartitionAndSortWithinPartitions(new FileGroupPartitioner(fileGroupCount), keyComparator)
        .map(t -> t._2);
    ValidationUtils.checkArgument(partitionedRDD.getNumPartitions() <= fileGroupCount,
        String.format("Partitioned RDD has more partitions %d than the fileGroupCount %d", partitionedRDD.getNumPartitions(), fileGroupCount));

    fileIDPfxs = partitionedRDD.mapPartitions(recordItr -> {
      // Due to partitioning, all record in the partition should have same fileID
      List<String> fileIds = new ArrayList<>(1);
      if (recordItr.hasNext()) {
        HoodieRecord record = recordItr.next();
        final String fileID = record.getCurrentLocation().getFileId();
        // Remove the write-token from the fileID as we need to return only the prefix
        int index = fileID.lastIndexOf("-");
        fileIds.add(fileID.substring(0, index));
      }
      // Remove the file-index since we want to
      return fileIds.iterator();
    }, true).collect();
    ValidationUtils.checkArgument(partitionedRDD.getNumPartitions() == fileIDPfxs.size(),
        String.format("Generated fileIDPfxs (%d) are lesser in size than the partitions %d", fileIDPfxs.size(), partitionedRDD.getNumPartitions()));

    return partitionedRDD;
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  @Override
  public List<String> generateFileIDPfxs(int numPartitions) {
    return fileIDPfxs;
  }
}
