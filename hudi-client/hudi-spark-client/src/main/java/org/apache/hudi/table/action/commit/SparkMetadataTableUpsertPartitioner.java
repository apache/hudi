package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;

import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Upsert Partitioner to be used for metadata table in spark. All records are prepped (location known) already wrt metadata table. So, we could optimize the upsert partitioner by avoiding certain
 * unnecessary computations.
 * @param <T>
 */
public class SparkMetadataTableUpsertPartitioner<T> extends SparkHoodiePartitioner<T> {

  private final List<BucketInfo> bucketInfoList;
  private final int totalPartitions;
  private final Map<String, Integer> fileIdToSparkPartitionIndexMap;

  public SparkMetadataTableUpsertPartitioner(List<BucketInfo> bucketInfoList, Map<String, Integer> fileIdToSparkPartitionIndexMap) {
    super(null, null);
    this.bucketInfoList = bucketInfoList;
    this.totalPartitions = bucketInfoList.size();
    this.fileIdToSparkPartitionIndexMap = fileIdToSparkPartitionIndexMap;
  }

  @Override
  public int numPartitions() {
    return totalPartitions;
  }

  @Override
  public int getPartition(Object key) {
    Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation = (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
    HoodieRecordLocation location = keyLocation._2().get();
    return fileIdToSparkPartitionIndexMap.get(location.getFileId());
  }

  @Override
  public BucketInfo getBucketInfo(int bucketNumber) {
    return bucketInfoList.get(bucketNumber);
  }
}
