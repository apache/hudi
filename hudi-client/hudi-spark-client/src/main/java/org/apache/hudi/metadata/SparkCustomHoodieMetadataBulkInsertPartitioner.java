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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import scala.Tuple3;

/**
 * A {@code BulkInsertPartitioner} implementation for Metadata Table to improve performance of initialization of metadata
 * table partition when a very large number of records are inserted.
 *
 * This partitioner requires the records to be already tagged with location.
 */
public class SparkCustomHoodieMetadataBulkInsertPartitioner implements BulkInsertPartitioner<JavaRDD<HoodieRecord>> {
  private final MetadataTableFileGroupIndexParser fileGroupIndexParser;
  public SparkCustomHoodieMetadataBulkInsertPartitioner(MetadataTableFileGroupIndexParser indexParser) {
    this.fileGroupIndexParser = indexParser;
  }

  private class FileGroupPartitioner extends Partitioner {

    @Override
    public int getPartition(Object key) {
      Tuple3<Integer, String, String> originalKey = (Tuple3<Integer, String, String>) key;
      return fileGroupIndexParser.getFileGroupIndex(originalKey._2(), originalKey._1());
    }

    @Override
    public int numPartitions() {
      return fileGroupIndexParser.getNumberOfFileGroups();
    }
  }

  // FileIDs for the various partitions
  private List<String> fileIDPfxs;

  /**
   * Partition the records by their location. The number of partitions is determined by the number of MDT fileGroups being udpated rather than the
   * specific value of outputSparkPartitions.
   */
  @Override
  public JavaRDD<HoodieRecord> repartitionRecords(JavaRDD<HoodieRecord> records, int outputSparkPartitions) {
    Comparator<Tuple3<Integer, String, String>> keyComparator =
        (Comparator<Tuple3<Integer, String, String>> & Serializable)(t1, t2) -> t1._3().compareTo(t2._3());

    // Partition the records by their file group
    JavaRDD<HoodieRecord> partitionedRDD = records
        // key by <file group index, record key>. The file group index is used to partition and the record key is used to sort within the partition.
        .keyBy(r -> {
          int fileGroupIndex = fileGroupIndexParser.getFileGroupIndex(r.getCurrentLocation().getFileId());
          String partitionPath = r.getPartitionPath();
          return new Tuple3<>(fileGroupIndex, partitionPath, r.getRecordKey());
        })
        .repartitionAndSortWithinPartitions(new FileGroupPartitioner(), keyComparator)
        .map(t -> t._2);

    fileIDPfxs = partitionedRDD.mapPartitions(recordItr -> {
      // Due to partitioning, all record in the partition should have same fileID. So we only can get the fileID prefix from the first record.
      List<String> fileIds = new ArrayList<>(1);
      if (recordItr.hasNext()) {
        HoodieRecord record = recordItr.next();
        final String fileID = HoodieTableMetadataUtil.getFileGroupPrefix(record.getCurrentLocation().getFileId());
        fileIds.add(fileID);
      } else {
        // FileGroupPartitioner returns a fixed number of partition as part of numPartitions(). In the special case that recordsRDD has fewer
        // records than fileGroupCount, some of these partitions (corresponding to fileGroups) will not have any data.
        // But we still need to return a fileID for use within {@code BulkInsertMapFunction}
        fileIds.add(StringUtils.EMPTY_STRING);
      }
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
  public String getFileIdPfx(int partitionId) {
    return fileIDPfxs.get(partitionId);
  }
}