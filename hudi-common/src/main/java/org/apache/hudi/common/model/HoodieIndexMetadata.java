/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS;

/**
 * Represents the metadata for all functional and secondary indexes in Hudi.
 */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieIndexMetadata implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieIndexMetadata.class);

  // Map to hold the index definitions keyed by their names.
  private Map<String, HoodieIndexDefinition> indexDefinitions;

  public HoodieIndexMetadata() {
    this.indexDefinitions = new HashMap<>();
  }

  public HoodieIndexMetadata(Map<String, HoodieIndexDefinition> indexDefinitions) {
    this.indexDefinitions = indexDefinitions;
    validateIndexMetadata(this);
  }

  public void setIndexDefinitions(Map<String, HoodieIndexDefinition> indexDefinitions) {
    this.indexDefinitions = indexDefinitions;
    validateIndexMetadata(this);
  }

  /**
   * Check if an index with the given name exists.
   *
   * @param indexName The name of the index to check.
   * @return true if the index exists, false otherwise.
   */
  public boolean hasIndex(String indexName) {
    return indexDefinitions != null && indexDefinitions.containsKey(indexName) && indexDefinitions.get(indexName) != null;
  }

  /**
   * Get the index definition for the given index name.
   *
   * @param indexName The name of the index to retrieve.
   * @return Option containing the index definition if it exists, Option.empty() otherwise.
   */
  public Option<HoodieIndexDefinition> getIndex(String indexName) {
    if (indexDefinitions != null && indexDefinitions.containsKey(indexName)) {
      HoodieIndexDefinition indexDef = indexDefinitions.get(indexName);
      if (indexDef != null) {
        return Option.of(indexDef);
      }
    }
    return Option.empty();
  }

  /**
   * Serialize this object to JSON string.
   *
   * @return Serialized JSON string.
   * @throws JsonProcessingException If any serialization errors occur.
   */
  public String toJson() throws JsonProcessingException {
    if (indexDefinitions.containsKey(null)) {
      LOG.info("null index name for the index definition " + indexDefinitions.get(null));
      indexDefinitions.remove(null);
    }
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  /**
   * Deserialize from JSON string to create an instance of this class.
   *
   * @param json Input JSON string.
   * @return Deserialized instance of HoodieIndexesMetadata.
   * @throws IOException If any deserialization errors occur.
   */
  public static HoodieIndexMetadata fromJson(String json) throws IOException {
    if (json == null || json.isEmpty()) {
      return new HoodieIndexMetadata();
    }
    return JsonUtils.getObjectMapper().readValue(json, HoodieIndexMetadata.class);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", HoodieIndexMetadata.class.getSimpleName() + "[", "]")
        .add("indexDefinitions=" + indexDefinitions)
        .toString();
  }

  public static void validateIndexMetadata(HoodieIndexMetadata indexMetadata) {
    // validate col stats and partition stats are on the same version
    Option<HoodieIndexDefinition> colStatsDef = indexMetadata.getIndex(PARTITION_NAME_COLUMN_STATS);
    Option<HoodieIndexDefinition> partitionStatsDef = indexMetadata.getIndex(PARTITION_NAME_PARTITION_STATS);
    if (colStatsDef.isPresent() && partitionStatsDef.isPresent()) {
      ValidationUtils.checkArgument(colStatsDef.get().getVersion().equals(partitionStatsDef.get().getVersion()),
          "Column stats and partition stats are not on the same version");
    }
  }
}
