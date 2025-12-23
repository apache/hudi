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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.exception.HoodieClusteringException;

import lombok.Getter;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ConsistentBucketIdentifier extends BucketIdentifier {

  /**
   * Hashing metadata of a partition
   */
  @Getter
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

  public int getNumBuckets() {
    return ring.size();
  }

  /**
   * Get bucket of the given file group
   *
   * @param fileId the file group id. NOTE: not filePrefix (i.e., uuid)
   */
  public ConsistentHashingNode getBucketByFileId(String fileId) {
    return fileIdToBucket.get(fileId);
  }

  public ConsistentHashingNode getBucket(HoodieKey hoodieKey, List<String> indexKeyFields) {
    return getBucket(getHashKeys(hoodieKey.getRecordKey(), indexKeyFields));
  }

  public ConsistentHashingNode getBucket(String recordKey, List<String> indexKeyFields) {
    return getBucket(getHashKeys(recordKey, indexKeyFields));
  }

  public ConsistentHashingNode getBucket(String recordKey, String indexKeyFields) {
    return getBucket(getHashKeys(recordKey, indexKeyFields));
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
   * Get the former node of the given node (inferred from file id).
   */
  public ConsistentHashingNode getFormerBucket(String fileId) {
    return getFormerBucket(getBucketByFileId(fileId).getValue());
  }

  /**
   * Get the former node of the given node (inferred from hash value).
   */
  public ConsistentHashingNode getFormerBucket(int hashValue) {
    SortedMap<Integer, ConsistentHashingNode> headMap = ring.headMap(hashValue);
    return headMap.isEmpty() ? ring.lastEntry().getValue() : headMap.get(headMap.lastKey());
  }

  /**
   * Get the latter node of the given node (inferred from file id).
   */
  public ConsistentHashingNode getLatterBucket(String fileId) {
    return getLatterBucket(getBucketByFileId(fileId).getValue());
  }

  /**
   * Get the latter node of the given node (inferred from hash value).
   */
  public ConsistentHashingNode getLatterBucket(int hashValue) {
    SortedMap<Integer, ConsistentHashingNode> tailMap = ring.tailMap(hashValue, false);
    return tailMap.isEmpty() ? ring.firstEntry().getValue() : tailMap.get(tailMap.firstKey());
  }

  public List<ConsistentHashingNode> mergeBucket(List<String> fileIds) {
    ValidationUtils.checkArgument(fileIds.size() >= 2, "At least two file groups should be provided for merging");
    // Get nodes using fileIds
    List<ConsistentHashingNode> nodes = fileIds.stream().map(this::getBucketByFileId).collect(Collectors.toList());

    // Validate the input
    for (int i = 0; i < nodes.size() - 1; ++i) {
      ValidationUtils.checkState(getFormerBucket(nodes.get(i + 1).getValue()).getValue() == nodes.get(i).getValue(), "Cannot merge discontinuous hash range");
    }

    // Create child nodes with proper tag (keep the last one and delete other nodes)
    List<ConsistentHashingNode> childNodes = new ArrayList<>(nodes.size());
    for (int i = 0; i < nodes.size() - 1; ++i) {
      childNodes.add(new ConsistentHashingNode(nodes.get(i).getValue(), null, ConsistentHashingNode.NodeTag.DELETE));
    }
    childNodes.add(new ConsistentHashingNode(nodes.get(nodes.size() - 1).getValue(), FSUtils.createNewFileIdPfx(), ConsistentHashingNode.NodeTag.REPLACE));
    return childNodes;
  }

  public Option<List<ConsistentHashingNode>> splitBucket(String fileId) {
    ConsistentHashingNode bucket = getBucketByFileId(fileId);
    ValidationUtils.checkState(bucket != null, "FileId has no corresponding bucket");
    return splitBucket(bucket);
  }

  /**
   * Split bucket in the range middle, also generate the corresponding file ids
   *
   * TODO support different split criteria, e.g., distribute records evenly using statistics
   *
   * @param bucket parent bucket
   * @return lists of children buckets
   */
  public Option<List<ConsistentHashingNode>> splitBucket(@Nonnull ConsistentHashingNode bucket) {
    ConsistentHashingNode formerBucket = getFormerBucket(bucket.getValue());

    long mid = (long) formerBucket.getValue() + bucket.getValue()
        + (formerBucket.getValue() < bucket.getValue() ? 0 : (HoodieConsistentHashingMetadata.HASH_VALUE_MASK + 1L));
    mid = (mid >> 1) & HoodieConsistentHashingMetadata.HASH_VALUE_MASK;

    // Cannot split as it already is the smallest bucket range
    if (mid == formerBucket.getValue() || mid == bucket.getValue()) {
      return Option.empty();
    }

    return Option.of(Arrays.asList(
        new ConsistentHashingNode((int) mid, FSUtils.createNewFileIdPfx(), ConsistentHashingNode.NodeTag.REPLACE),
        new ConsistentHashingNode(bucket.getValue(), FSUtils.createNewFileIdPfx(), ConsistentHashingNode.NodeTag.REPLACE))
    );
  }

  /**
   * Initialize necessary data structure to facilitate bucket identifying.
   * Specifically, we construct:
   * - An in-memory tree (ring) to speed up range mapping searching.
   * - A hash table (fileIdToBucket) to allow lookup of bucket using fileId.
   * <p>
   * Children nodes are also considered, and will override the original nodes,
   * which is used during bucket resizing (i.e., children nodes take the place
   * of the original nodes)
   */
  private void initialize() {
    for (ConsistentHashingNode p : metadata.getNodes()) {
      ring.put(p.getValue(), p);
      // One bucket has only one file group, so append 0 directly
      fileIdToBucket.put(FSUtils.createNewFileId(p.getFileIdPrefix(), 0), p);
    }

    // Handle children nodes, i.e., replace or delete the original nodes
    ConsistentHashingNode tmp;
    for (ConsistentHashingNode p : metadata.getChildrenNodes()) {
      switch (p.getTag()) {
        case REPLACE:
          tmp = ring.put(p.getValue(), p);
          if (tmp != null) {
            fileIdToBucket.remove(FSUtils.createNewFileId(tmp.getFileIdPrefix(), 0));
          }
          fileIdToBucket.put(FSUtils.createNewFileId(p.getFileIdPrefix(), 0), p);
          break;
        case DELETE:
          tmp = ring.remove(p.getValue());
          fileIdToBucket.remove(FSUtils.createNewFileId(tmp.getFileIdPrefix(), 0));
          break;
        default:
          throw new HoodieClusteringException("Children node is tagged as NORMAL or unknown tag: " + p);
      }
    }
  }
}
