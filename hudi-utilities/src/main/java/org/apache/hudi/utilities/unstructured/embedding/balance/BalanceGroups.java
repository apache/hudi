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

package org.apache.hudi.utilities.unstructured.embedding.balance;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.prep.ImageInputPreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.JcodecVideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFilePreprocessor;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The balancing group of a prep row: the preprocessor its {@code source_file_extension} (lower-cased) resolves to in the job's
 * {@link PreprocessorRegistry}, named by a stable alias rather than {@code FilePreprocessor#name()} (which carries a version
 * and, for PDF, the runtime PDFBox version, so a key built from it would stop matching after a library bump):
 * {@link #PDF} for the shipped PDF preprocessor, {@link #IMAGE} for the known image preprocessor class, {@link #VIDEO} for the
 * shipped video preprocessors and the job's configured video preprocessor, the fully qualified class name for any other preprocessor, and
 * {@link #UNMAPPED} for a null extension or one no preprocessor resolves to. Built on the driver from the job's properties;
 * no preprocessor is instantiated.
 */
public final class BalanceGroups {

  public static final String PDF = "pdf";
  public static final String IMAGE = "image";
  public static final String VIDEO = "video";
  public static final String UNMAPPED = "unmapped";

  private static final List<String> SHIPPED_VIDEO_CLASSES =
      Arrays.asList(VideoFramePreprocessor.class.getName(), JcodecVideoFramePreprocessor.class.getName());

  private final Map<String, String> groupByExtension;

  private BalanceGroups(Map<String, String> groupByExtension) {
    this.groupByExtension = Collections.unmodifiableMap(groupByExtension);
  }

  public static BalanceGroups from(TypedProperties props) {
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    Set<String> videoClasses = new HashSet<>(SHIPPED_VIDEO_CLASSES);
    videoClasses.add(registry.videoClassName());
    Map<String, String> groups = new TreeMap<>();
    for (Map.Entry<String, String> entry : registry.classNamesByExtension().entrySet()) {
      groups.put(entry.getKey(), groupOfClass(entry.getValue(), videoClasses));
    }
    return new BalanceGroups(groups);
  }

  private static String groupOfClass(String className, Set<String> videoClasses) {
    if (PdfFilePreprocessor.class.getName().equals(className)) {
      return PDF;
    }
    if (ImageInputPreprocessor.class.getName().equals(className)) {
      return IMAGE;
    }
    return videoClasses.contains(className) ? VIDEO : className;
  }

  /**
   * @return the group of a file extension, compared case-insensitively; {@link #UNMAPPED} for null or an unresolved extension
   */
  public String groupOf(String extension) {
    if (extension == null) {
      return UNMAPPED;
    }
    return groupByExtension.getOrDefault(extension.toLowerCase(Locale.ROOT), UNMAPPED);
  }

  /** @return lower-case extension to group, sorted by extension */
  public Map<String, String> byExtension() {
    return groupByExtension;
  }

  /** @return the groups some extension of this job resolves to ({@link #UNMAPPED} is not one), sorted */
  public Set<String> configuredGroups() {
    return Collections.unmodifiableSet(new TreeSet<>(groupByExtension.values()));
  }

  /**
   * @return the group of {@code fileExtension} as a Spark expression, the same mapping as {@link #groupOf}: a null
   *     extension matches no branch and falls to {@link #UNMAPPED}
   */
  public Column groupColumn(Column fileExtension) {
    Column lower = functions.lower(fileExtension);
    Column group = null;
    for (Map.Entry<String, String> entry : groupByExtension.entrySet()) {
      Column match = lower.equalTo(functions.lit(entry.getKey()));
      group = group == null ? functions.when(match, entry.getValue()) : group.when(match, entry.getValue());
    }
    return group == null ? functions.lit(UNMAPPED) : group.otherwise(functions.lit(UNMAPPED));
  }
}
