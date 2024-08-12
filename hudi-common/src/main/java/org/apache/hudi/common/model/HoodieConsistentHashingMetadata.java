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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.JsonUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * All the metadata that is used for consistent hashing bucket index
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieConsistentHashingMetadata implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieConsistentHashingMetadata.class);
  /**
   * Upper-bound of the hash value
   */
  public static final int HASH_VALUE_MASK = Integer.MAX_VALUE;
  public static final String HASHING_METADATA_FILE_SUFFIX = ".hashing_meta";
  public static final String HASHING_METADATA_COMMIT_FILE_SUFFIX = ".commit";

  private final short version;
  private final String partitionPath;
  private final String instant;
  private final int numBuckets;
  private final int seqNo;
  private final List<ConsistentHashingNode> nodes;
  @JsonIgnore
  protected List<ConsistentHashingNode> childrenNodes = new ArrayList<>();
  /**
   * Used to indicate if the metadata is newly created, rather than read from the storage.
   */
  @JsonIgnore
  private boolean firstCreated;

  @JsonCreator
  public HoodieConsistentHashingMetadata(@JsonProperty("version") short version, @JsonProperty("partitionPath") String partitionPath,
                                         @JsonProperty("instant") String instant, @JsonProperty("numBuckets") int numBuckets,
                                         @JsonProperty("seqNo") int seqNo, @JsonProperty("nodes") List<ConsistentHashingNode> nodes) {
    this.version = version;
    this.partitionPath = partitionPath;
    this.instant = instant;
    this.numBuckets = numBuckets;
    this.seqNo = seqNo;
    this.nodes = nodes;
    this.firstCreated = false;
  }

  /**
   * Only used for creating new hashing metadata.
   * Construct default metadata with all bucket's file group uuid initialized
   */
  public HoodieConsistentHashingMetadata(String partitionPath, int numBuckets) {
    this((short) 0, partitionPath, HoodieTimeline.INIT_INSTANT_TS, numBuckets, 0, constructDefaultHashingNodes(partitionPath, numBuckets));
    this.firstCreated = true;
  }

  private static List<ConsistentHashingNode> constructDefaultHashingNodes(String partitionPath, int numBuckets) {
    long step = ((long) HASH_VALUE_MASK + numBuckets - 1) / numBuckets;
    long bucketStart = 0;
    List<ConsistentHashingNode> nodes = new ArrayList<>(numBuckets);
    for (int idx = 1; idx < numBuckets + 1; idx++) {
      long bucketEnd = Math.min(step * idx, HASH_VALUE_MASK);
      String fileId = generateUUID(partitionPath, bucketStart, bucketEnd);
      nodes.add(new ConsistentHashingNode((int) bucketEnd, fileId));
      bucketStart = bucketEnd;
    }
    return nodes;
  }

  private static String generateUUID(String partitionPath, long bucketStart, long bucketEnd) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(16);
    byteBuffer.putLong(bucketStart);
    byteBuffer.putLong(bucketEnd);
    byte[] longBytes = byteBuffer.array();
    byte[] partitionPathBytes = getUTF8Bytes(partitionPath);
    byte[] combinedBytes = new byte[longBytes.length + partitionPathBytes.length];
    System.arraycopy(longBytes, 0, combinedBytes, 0, longBytes.length);
    System.arraycopy(partitionPathBytes, 0, combinedBytes, longBytes.length, partitionPathBytes.length);
    return UUID.nameUUIDFromBytes(combinedBytes).toString();
  }

  public short getVersion() {
    return version;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getInstant() {
    return instant;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public int getSeqNo() {
    return seqNo;
  }

  public boolean isFirstCreated() {
    return firstCreated;
  }

  public List<ConsistentHashingNode> getNodes() {
    return nodes;
  }

  public List<ConsistentHashingNode> getChildrenNodes() {
    return childrenNodes;
  }

  public void setChildrenNodes(List<ConsistentHashingNode> childrenNodes) {
    this.childrenNodes = childrenNodes;
  }

  public String getFilename() {
    return instant + HASHING_METADATA_FILE_SUFFIX;
  }

  public byte[] toBytes() throws IOException {
    return getUTF8Bytes(toJsonString());
  }

  public static HoodieConsistentHashingMetadata fromBytes(byte[] bytes) throws IOException {
    try {
      return fromJsonString(fromUTF8Bytes(bytes), HoodieConsistentHashingMetadata.class);
    } catch (Exception e) {
      throw new IOException("unable to read hashing metadata", e);
    }
  }

  private String toJsonString() throws IOException {
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  protected static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file (no data or something bad happen).
      return clazz.newInstance();
    }
    return JsonUtils.getObjectMapper().readValue(jsonStr, clazz);
  }

  /**
   * Get instant time from the hashing metadata filename
   * Pattern of the filename: <instant>.HASHING_METADATA_FILE_SUFFIX
   */
  public static String getTimestampFromFile(String filename) {
    return filename.split("\\.")[0];
  }
}
