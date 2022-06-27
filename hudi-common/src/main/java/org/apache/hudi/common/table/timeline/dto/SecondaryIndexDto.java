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

package org.apache.hudi.common.table.timeline.dto;

import org.apache.hudi.secondary.index.HoodieSecondaryIndex;
import org.apache.hudi.secondary.index.SecondaryIndexType;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SecondaryIndexDto implements Serializable {
  @JsonProperty("indexName")
  private String indexName;

  @JsonProperty("indexType")
  private byte indexType;

  @JsonProperty("columns")
  private LinkedHashMap<String, Map<String, String>> columns;

  @JsonProperty("options")
  private Map<String, String> options;

  public static SecondaryIndexDto fromSecondaryIndex(HoodieSecondaryIndex secondaryIndex) {
    SecondaryIndexDto dto = new SecondaryIndexDto();
    dto.indexName = secondaryIndex.getIndexName();
    dto.indexType = secondaryIndex.getIndexType().getValue();
    dto.columns = secondaryIndex.getColumns();
    dto.options = secondaryIndex.getOptions();
    return dto;
  }

  public static HoodieSecondaryIndex toSecondaryIndex(SecondaryIndexDto dto) {
    SecondaryIndexType indexType = SecondaryIndexType.of(dto.indexType);
    return new HoodieSecondaryIndex(dto.indexName, indexType, dto.columns, dto.options);
  }
}
