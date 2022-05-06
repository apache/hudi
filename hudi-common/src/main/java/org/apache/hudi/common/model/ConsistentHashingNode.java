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

import org.apache.hudi.common.util.JsonUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Used in consistent hashing index, representing nodes in the consistent hash ring.
 * Record the end hash range value and its corresponding file group id.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsistentHashingNode implements Serializable {

  private final int value;
  private final String fileIdPrefix;

  @JsonCreator
  public ConsistentHashingNode(@JsonProperty("value") int value, @JsonProperty("fileIdPrefix") String fileIdPrefix) {
    this.value = value;
    this.fileIdPrefix = fileIdPrefix;
  }

  public static String toJsonString(List<ConsistentHashingNode> nodes) throws IOException {
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(nodes);
  }

  public static List<ConsistentHashingNode> fromJsonString(String json) throws Exception {
    if (json == null || json.isEmpty()) {
      return Collections.emptyList();
    }

    ConsistentHashingNode[] nodes = JsonUtils.getObjectMapper().readValue(json, ConsistentHashingNode[].class);
    return Arrays.asList(nodes);
  }

  public int getValue() {
    return value;
  }

  public String getFileIdPrefix() {
    return fileIdPrefix;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ConsistentHashingNode{");
    sb.append("value=").append(value);
    sb.append(", fileIdPfx='").append(fileIdPrefix).append('\'');
    sb.append('}');
    return sb.toString();
  }
}