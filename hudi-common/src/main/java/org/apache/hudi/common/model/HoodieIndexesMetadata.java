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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the metadata for all functional and secondary indexes in Hudi.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieIndexesMetadata implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieIndexesMetadata.class);

  // Map to hold the index definitions keyed by their names.
  private Map<String, HoodieIndexDefinition> indexDefinitions;

  public HoodieIndexesMetadata() {
    this.indexDefinitions = new HashMap<>();
  }

  public HoodieIndexesMetadata(Map<String, HoodieIndexDefinition> indexDefinitions) {
    this.indexDefinitions = indexDefinitions;
  }

  public Map<String, HoodieIndexDefinition> getIndexDefinitions() {
    return indexDefinitions;
  }

  public void setIndexDefinitions(Map<String, HoodieIndexDefinition> indexDefinitions) {
    this.indexDefinitions = indexDefinitions;
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
  public static HoodieIndexesMetadata fromJson(String json) throws IOException {
    if (json == null || json.isEmpty()) {
      return new HoodieIndexesMetadata();
    }
    return JsonUtils.getObjectMapper().readValue(json, HoodieIndexesMetadata.class);
  }

  @Override
  public String toString() {
    return "HoodieIndexesMetadata{"
        + "indexDefinitions=" + indexDefinitions
        + '}';
  }
}
