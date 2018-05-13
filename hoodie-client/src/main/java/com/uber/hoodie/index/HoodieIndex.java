/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.index;

import com.google.common.base.Optional;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieIndexException;
import com.uber.hoodie.index.bloom.HoodieBloomIndex;
import com.uber.hoodie.index.bucketed.BucketedIndex;
import com.uber.hoodie.index.hbase.HBaseIndex;
import com.uber.hoodie.table.HoodieTable;
import java.io.Serializable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Base class for different types of indexes to determine the mapping from uuid
 */
public abstract class HoodieIndex<T extends HoodieRecordPayload> implements Serializable {

  protected final HoodieWriteConfig config;

  protected HoodieIndex(HoodieWriteConfig config) {
    this.config = config;
  }


  public static <T extends HoodieRecordPayload> HoodieIndex<T> createIndex(HoodieWriteConfig config,
      JavaSparkContext jsc) throws HoodieIndexException {
    switch (config.getIndexType()) {
      case HBASE:
        return new HBaseIndex<>(config);
      case INMEMORY:
        return new InMemoryHashIndex<>(config);
      case BLOOM:
        return new HoodieBloomIndex<>(config);
      case BUCKETED:
        return new BucketedIndex<>(config);
      default:
        throw new HoodieIndexException("Index type unspecified, set " + config.getIndexType());
    }
  }

  /**
   * Checks if the given [Keys] exists in the hoodie table and returns [Key, Optional[FullFilePath]]
   * If the optional FullFilePath value is not present, then the key is not found. If the
   * FullFilePath value is present, it is the path component (without scheme) of the URI underlying
   * file
   */
  public abstract JavaPairRDD<HoodieKey, Optional<String>> fetchRecordLocation(
      JavaRDD<HoodieKey> hoodieKeys, final JavaSparkContext jsc, HoodieTable<T> hoodieTable);

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains the
   * row (if it is actually present)
   */
  public abstract JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD,
      JavaSparkContext jsc, HoodieTable<T> hoodieTable) throws HoodieIndexException;

  /**
   * Extracts the location of written records, and updates the index.
   * <p>
   * TODO(vc): We may need to propagate the record as well in a WriteStatus class
   */
  public abstract JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc,
      HoodieTable<T> hoodieTable)
      throws HoodieIndexException;

  /**
   * Rollback the efffects of the commit made at commitTime.
   */
  public abstract boolean rollbackCommit(String commitTime);

  /**
   * An index is `global` if {@link HoodieKey} to fileID mapping, does not depend on the
   * `partitionPath`. Such an implementation is able to obtain the same mapping, for two hoodie keys
   * with same `recordKey` but different `partitionPath`
   *
   * @return whether or not, the index implementation is global in nature
   */
  public abstract boolean isGlobal();

  /**
   * This is used by storage to determine, if its safe to send inserts, straight to the log, i.e
   * having a {@link com.uber.hoodie.common.model.FileSlice}, with no data file.
   *
   * @return Returns true/false depending on whether the impl has this capability
   */
  public abstract boolean canIndexLogFiles();


  /**
   * An index is "implicit" with respect to storage, if just writing new data to a file slice,
   * updates the index as well. This is used by storage, to save memory footprint in certain cases.
   */
  public abstract boolean isImplicitWithStorage();


  public enum IndexType {
    HBASE, INMEMORY, BLOOM, BUCKETED
  }
}
