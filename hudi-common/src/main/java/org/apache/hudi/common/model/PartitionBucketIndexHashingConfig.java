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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionBucketIndexHashingConfig implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketIndexHashingConfig.class);
  public static final String HASHING_CONFIG_FILE_SUFFIX = ".hashing_config";
  public static final Integer CURRENT_VERSION = 1;
  private final String expressions;
  private final int defaultBucketNumber;
  private final String rule;

  private final int version;
  private final String instant;

  @JsonCreator
  public PartitionBucketIndexHashingConfig(@JsonProperty("expressions") String expressions,
                                           @JsonProperty("defaultBucketNumber") int defaultBucketNumber,
                                           @JsonProperty("rule") String rule,
                                           @JsonProperty("version") int version,
                                           @JsonProperty("instant") String instant) {
    this.expressions = expressions;
    this.defaultBucketNumber = defaultBucketNumber;
    this.rule = rule;
    this.version = version;
    this.instant = instant;
  }

  public String getFilename() {
    return instant + HASHING_CONFIG_FILE_SUFFIX;
  }

  public String toJsonString() throws IOException {
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file (no data or somethings bad happen).
      return clazz.newInstance();
    }
    return JsonUtils.getObjectMapper().readValue(jsonStr, clazz);
  }


  public static PartitionBucketIndexHashingConfig fromBytes(byte[] bytes) throws IOException {
    try {
      return fromJsonString(new String(bytes, StandardCharsets.UTF_8), PartitionBucketIndexHashingConfig.class);
    } catch (Exception e) {
      throw new IOException("unable to load hashing config", e);
    }
  }

  public int getVersion() {
    return version;
  }

  public String getRule() {
    return rule;
  }

  public int getDefaultBucketNumber() {
    return defaultBucketNumber;
  }

  public String getExpressions() {
    return expressions;
  }
}
