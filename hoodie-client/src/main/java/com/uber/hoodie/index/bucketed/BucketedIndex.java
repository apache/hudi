/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.index.bucketed;

import com.google.common.base.Optional;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieIndexException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * An `stateless` index implementation that will using a deterministic mapping function to determine
 * the fileID for a given record.
 * <p>
 * Pros: - Fast
 * <p>
 * Cons : - Need to tune the number of buckets per partition path manually (FIXME: Need to autotune
 * this) - Could increase write amplification on copy-on-write storage since inserts always rewrite
 * files - Not global.
 */
public class BucketedIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

  private static Logger logger = LogManager.getLogger(BucketedIndex.class);

  public BucketedIndex(HoodieWriteConfig config) {
    super(config);
  }

  private String getBucket(String recordKey) {
    return String.valueOf(recordKey.hashCode() % config.getNumBucketsPerPartition());
  }

  @Override
  public JavaPairRDD<HoodieKey, Optional<String>> fetchRecordLocation(JavaRDD<HoodieKey> hoodieKeys,
      JavaSparkContext jsc, HoodieTable<T> hoodieTable) {
    return hoodieKeys.mapToPair(hk -> new Tuple2<>(hk, Optional.of(getBucket(hk.getRecordKey()))));
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable)
      throws HoodieIndexException {
    return recordRDD.map(record -> {
      String bucket = getBucket(record.getRecordKey());
      //HACK(vc) a non-existent commit is provided here.
      record.setCurrentLocation(new HoodieRecordLocation("000", bucket));
      return record;
    });
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable)
      throws HoodieIndexException {
    return writeStatusRDD;
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    // nothing to rollback in the index.
    return true;
  }

  /**
   * Bucketing is still done within each partition.
   */
  @Override
  public boolean isGlobal() {
    return false;
  }

  /**
   * Since indexing is just a deterministic hash, we can identify file group correctly even without
   * an index on the actual log file.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Indexing is just a hash function.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }
}
