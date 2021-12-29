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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * All the metadata that is used for consistent hashing bucket index
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieConsistentHashingMetadata implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieConsistentHashingMetadata.class);

  public static final int MAX_HASH_VALUE = Integer.MAX_VALUE;
  public static final String HASHING_METADATA_FILE_SUFFIX = ".hashing_meta";

  protected short version;
  protected String partitionPath;
  protected String instant;
  protected int numBuckets;
  protected int seqNo;
  protected List<ConsistentHashingNode> nodes;

  public HoodieConsistentHashingMetadata() {
  }

  public HoodieConsistentHashingMetadata(String partitionPath, int numBuckets) {
    this((short) 0, partitionPath, HoodieTimeline.INIT_INSTANT_TS, numBuckets);
  }

  /**
   * Construct default metadata with all bucket's file group uuid initialized
   *
   * @param partitionPath
   * @param numBuckets
   */
  private HoodieConsistentHashingMetadata(short version, String partitionPath, String instant, int numBuckets) {
    this.version = version;
    this.partitionPath = partitionPath;
    this.instant = instant;
    this.numBuckets = numBuckets;

    nodes = new ArrayList<>();
    long step = ((long) MAX_HASH_VALUE + numBuckets - 1) / numBuckets;
    for (int i = 1; i <= numBuckets; ++i) {
      nodes.add(new ConsistentHashingNode((int) Math.min(step * i, MAX_HASH_VALUE), FSUtils.createNewFileIdPfx()));
    }
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

  public List<ConsistentHashingNode> getNodes() {
    return nodes;
  }

  public void setInstant(String instant) {
    this.instant = instant;
  }

  public void setNodes(List<ConsistentHashingNode> nodes) {
    this.nodes = nodes;
    this.numBuckets = nodes.size();
  }

  public void setSeqNo(int seqNo) {
    this.seqNo = seqNo;
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
    return getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  protected static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file (no data or somethings bad happen).
      return clazz.newInstance();
    }
    return getObjectMapper().readValue(jsonStr, clazz);
  }

  protected static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper;
  }

  /**
   * Get instant time from the hashing metadata filename
   * Filename pattern: <instant>.HASHING_METADATA_FILE_SUFFIX
   *
   * @param filename
   * @return
   */
  public static String getTimestampFromFile(String filename) {
    return filename.split("\\.")[0];
  }
}
