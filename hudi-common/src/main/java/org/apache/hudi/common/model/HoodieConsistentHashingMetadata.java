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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * All the metadata that is used for consistent hashing bucket index
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieConsistentHashingMetadata implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieConsistentHashingMetadata.class);
  /**
   * Upper-bound of the hash value
   */
  public static final int HASH_VALUE_MASK = Integer.MAX_VALUE;
  public static final String HASHING_METADATA_FILE_SUFFIX = ".hashing_meta";

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

  protected static List<ConsistentHashingNode> constructDefaultHashingNodes(String partitionPath, int numBuckets) {
    long step = ((long) HASH_VALUE_MASK + numBuckets - 1) / numBuckets;
    String uuidPostfix = partitionPathToUuidPostfix(partitionPath);
    return IntStream.range(1, numBuckets + 1)
        .mapToObj(i -> new ConsistentHashingNode((int) Math.min(step * i, HASH_VALUE_MASK), String.format("%08d-%s", i - 1, uuidPostfix))).collect(Collectors.toList());
  }

  /**
   * Format partition path to simulate an UUID. E.g.,
   *  - part2/part3       -> 0000-0000-0000-00part2part3
   *  - part1/part2/part3 -> 0000-0000-0par-t1part2part3
   *
   *  @VisibleForTesting
   */
  protected static String partitionPathToUuidPostfix(String partitionPath) {
    String reveredPartition = new StringBuilder(partitionPath.replace("/", ""))
        .reverse().toString();
    reveredPartition = reveredPartition.substring(0, Math.min(24, reveredPartition.length()));

    String ret = new StringBuilder(reveredPartition)
        .append(String.join("", Collections.nCopies(24 - reveredPartition.length(), "0")))
        .reverse().toString();

    return ret.substring(0, 4) + "-" + ret.substring(4, 8) + "-" + ret.substring(8, 12) + "-" + ret.substring(12, 24);
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
    return toJsonString().getBytes(StandardCharsets.UTF_8);
  }

  public static HoodieConsistentHashingMetadata fromBytes(byte[] bytes) throws IOException {
    try {
      return fromJsonString(new String(bytes, StandardCharsets.UTF_8), HoodieConsistentHashingMetadata.class);
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
