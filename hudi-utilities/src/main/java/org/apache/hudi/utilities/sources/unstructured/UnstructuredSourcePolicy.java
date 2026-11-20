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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Construction-time file-type policy for raw PDF, image and video ingestion. */
public final class UnstructuredSourcePolicy {
  private static final Set<String> COLUMNAR_EXTENSIONS = Set.of(
      "parquet", "orc", "avro", "json", "jsonl", "csv", "xml", "hfile");

  /**
   * The source name the one-argument {@link #fileExtensions(TypedProperties)} puts in its refusals: the unstructured data source
   * without its mode, because that caller knows no particular job.
   */
  private static final String ANY_UNSTRUCTURED_SOURCE = "UnstructuredDataSource";

  private UnstructuredSourcePolicy() {
  }

  /**
   * {@link #fileExtensions(TypedProperties, String)} for a caller that runs after the job's source validated the same properties
   * ({@code BlobSourceSetup.validate}), so its refusals do not fire there.
   */
  public static List<String> fileExtensions(TypedProperties props) {
    return fileExtensions(props, ANY_UNSTRUCTURED_SOURCE);
  }

  /**
   * Returns the supported case-sensitive types, intersected with each configured extension list. Refusals name {@code source},
   * the source with its mode.
   * Disjoint valid lists return an empty selection; callers must preserve that empty selection.
   *
   * @throws IllegalArgumentException for a nonempty explicit datafile format or an unusable,
   *                                 structured or unsupported extension list
   */
  public static List<String> fileExtensions(TypedProperties props, String source) {
    String format = ConfigUtils.getStringWithAltKeys(props, CloudSourceConfig.DATAFILE_FORMAT, "");
    if (!format.isEmpty()) {
      throw new IllegalArgumentException(CloudSourceConfig.DATAFILE_FORMAT.key()
          + " must be empty for " + source + "; the source reads raw unstructured bytes");
    }
    List<String> allowed = Arrays.asList(BlobTableConfig.FILE_EXTENSIONS.defaultValue().split(","));
    List<String> selected = new ArrayList<>(allowed);
    if (props.containsKey(BlobTableConfig.FILE_EXTENSIONS.key())) {
      List<String> configured = BlobTableConfig.fileExtensions(props);
      validate(configured, allowed, source);
      selected.retainAll(configured);
    }
    String cloud = ConfigUtils.getStringWithAltKeys(props, CloudSourceConfig.CLOUD_DATAFILE_EXTENSION, (String) null);
    if (cloud != null) {
      // a leading dot is accepted, as the columnar cloud selection accepts it
      List<String> configured = Arrays.stream(cloud.split(",")).map(String::trim)
          .map(value -> value.startsWith(".") ? value.substring(1) : value)
          .filter(value -> !value.isEmpty()).collect(Collectors.toList());
      validate(configured, allowed, source);
      selected.retainAll(configured);
    }
    return selected;
  }

  private static void validate(List<String> configured, List<String> allowed, String source) {
    if (configured.isEmpty()) {
      throw new IllegalArgumentException(source + ": unstructured file extensions must narrow the supported PDF, image and video types");
    }
    for (String extension : configured) {
      if (COLUMNAR_EXTENSIONS.contains(extension)) {
        throw new IllegalArgumentException("Structured file extension is not supported by " + source + ": " + extension);
      }
      if (!allowed.contains(extension)) {
        throw new IllegalArgumentException(source + ": unsupported unstructured file extension: " + extension + "; allowed types: " + allowed);
      }
    }
  }
}
