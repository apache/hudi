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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.hash.HashID;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentBucketIdentifier extends BucketIdentifier {

  /**
   * Hashing metadata of a partition
   */
  private final HoodieConsistentHashingMetadata metadata;
  /**
   * In-memory structure to speed up ring mapping (hashing value -> hashing node)
   */
  private final TreeMap<Integer, ConsistentHashingNode> ring;
  /**
   * Mapping from fileId -> hashing node
   */
  private final Map<String, ConsistentHashingNode> fileIdToBucket;

  public ConsistentBucketIdentifier(HoodieConsistentHashingMetadata metadata) {
    this.metadata = metadata;
    this.fileIdToBucket = new HashMap<>();
    this.ring = new TreeMap<>();
    initialize();
  }

  public Collection<ConsistentHashingNode> getNodes() {
    return ring.values();
  }

  public HoodieConsistentHashingMetadata getMetadata() {
    return metadata;
  }

  public int getNumBuckets() {
    return ring.size();
  }

  /**
   * Get bucket of the given file group
   *
   * @param fileId the file group id. NOTE: not filePfx (i.e., uuid)
   */
  public ConsistentHashingNode getBucketByFileId(String fileId) {
    return fileIdToBucket.get(fileId);
  }

  public ConsistentHashingNode getBucket(HoodieKey hoodieKey, List<String> indexKeyFields) {
    return getBucket(getHashKeys(hoodieKey.getRecordKey(), indexKeyFields));
  }

  protected ConsistentHashingNode getBucket(List<String> hashKeys) {
    int hashValue = HashID.getXXHash32(String.join("", hashKeys), 0);
    return getBucket(hashValue & HoodieConsistentHashingMetadata.HASH_VALUE_MASK);
  }

  protected ConsistentHashingNode getBucket(int hashValue) {
    SortedMap<Integer, ConsistentHashingNode> tailMap = ring.tailMap(hashValue);
    return tailMap.isEmpty() ? ring.firstEntry().getValue() : tailMap.get(tailMap.firstKey());
  }

  /**
   * Initialize necessary data structure to facilitate bucket identifying.
   * Specifically, we construct:
   * - An in-memory tree (ring) to speed up range mapping searching.
   * - A hash table (fileIdToBucket) to allow lookup of bucket using fileId.
   */
  private void initialize() {
    for (ConsistentHashingNode p : metadata.getNodes()) {
      ring.put(p.getValue(), p);
      // One bucket has only one file group, so append 0 directly
      fileIdToBucket.put(FSUtils.createNewFileId(p.getFileIdPrefix(), 0), p);
    }
  }
}
