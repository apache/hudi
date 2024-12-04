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

/**
 * Sort marker. Mark the file with sort columns, sort mode and sort order.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SortMarker implements Serializable {

  public static final String SORT_MARKER_METADATA_KEY = "sort_marker";

  private final String[] sortColumns;
  private final SortMode sortMode;
  private final SortOrder sortOrder;

  @JsonCreator
  public SortMarker(@JsonProperty("sortColumns") String[] sortColumns, @JsonProperty("sortMode") SortMode sortMode, @JsonProperty("sortOrder") SortOrder sortOrder) {
    this.sortColumns = sortColumns;
    this.sortMode = sortMode;
    this.sortOrder = sortOrder;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public enum SortOrder implements Serializable {
    ASC, DESC
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public enum SortMode implements Serializable {
    LINEAR, Z_ORDER, HILBERT
  }

  public static SortMarker fromJsonString(String jsonStr) throws Exception {
    return JsonUtils.getObjectMapper().readValue(jsonStr, SortMarker.class);
  }

  public String toJsonString() throws IOException {
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public SortMode getSortMode() {
    return sortMode;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }

  public String[] getSortColumns() {
    return sortColumns;
  }

  public boolean match(SortMarker other) {
    if (other == null) {
      return false;
    }
    return this.sortMode == other.sortMode && this.sortOrder == other.sortOrder
        && this.sortColumns.length == other.sortColumns.length
        && this.sortColumns.equals(other.sortColumns);
  }

  public static final SortMarker of(String[] sortColumns, SortMode sortMode, SortOrder sortOrder) {
    return new SortMarker(sortColumns, sortMode, sortOrder);
  }
}
