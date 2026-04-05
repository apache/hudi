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

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.common.util.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hoodie file format.
 */
@EnumDescription("Hoodie file formats.")
public enum HoodieFileFormat {

  @EnumFieldDescription("Apache Parquet is an open source, column-oriented data file format "
      + "designed for efficient data storage and retrieval. It provides efficient data "
      + "compression and encoding schemes with enhanced performance to handle complex data in bulk.")
  PARQUET(".parquet"),

  @EnumFieldDescription("File format used for changes in MOR and archived timeline.")
  HOODIE_LOG(".log"),

  @EnumFieldDescription("(internal config) File format for metadata table. A file of sorted "
      + "key/value pairs. Both keys and values are byte arrays.")
  HFILE(".hfile"),

  @EnumFieldDescription("The Optimized Row Columnar (ORC) file format provides a highly efficient "
      + "way to store Hive data. It was designed to overcome limitations of the other Hive file "
      + "formats. Using ORC files improves performance when Hive is reading, writing, and "
      + "processing data.")
  ORC(".orc"),

  @EnumFieldDescription("Lance is a modern columnar data format optimized for random access patterns "
          + "and designed for ML and AI workloads")
  LANCE(".lance");

  public static final String LANCE_SPARK_ONLY_ERROR_MSG =
      "Lance base file format is currently only supported with the Spark engine. "
          + "Please use Parquet, ORC, or HFile for non-Spark engines (Flink, Hive, Presto, Trino).";

  public static final Set<String> BASE_FILE_EXTENSIONS = Arrays.stream(HoodieFileFormat.values())
      .map(HoodieFileFormat::getFileExtension)
      .filter(x -> !x.equals(HoodieFileFormat.HOODIE_LOG.getFileExtension()))
      .collect(Collectors.toCollection(HashSet::new));

  private final String extension;

  HoodieFileFormat(String extension) {
    this.extension = extension;
  }

  public String getFileExtension() {
    return extension;
  }

  /**
   * Returns true if this file format requires the SPARK record type for reading/writing.
   * Lance only supports the Spark-native InternalRow representation, not Avro.
   */
  public boolean requiresSparkRecordType() {
    return this == LANCE;
  }

  /**
   * Resolves the record type to use for this file format: returns SPARK if this format
   * requires it, otherwise returns the given fallback type.
   */
  public HoodieRecord.HoodieRecordType resolveRecordType(HoodieRecord.HoodieRecordType fallback) {
    return requiresSparkRecordType() ? HoodieRecord.HoodieRecordType.SPARK : fallback;
  }

  public static HoodieFileFormat fromFileExtension(String extension) {
    for (HoodieFileFormat format : HoodieFileFormat.values()) {
      if (format.getFileExtension().equals(extension)) {
        return format;
      }
    }
    throw new IllegalArgumentException("Unknown file extension :" + extension);
  }

  /**
   * Returns the {@link HoodieFileFormat} matching the given file extension, or {@code null}
   * if no match is found. Useful when the caller wants to handle unknown extensions without
   * exception-based control flow.
   */
  public static HoodieFileFormat fromFileExtensionOrNull(String extension) {
    for (HoodieFileFormat format : HoodieFileFormat.values()) {
      if (format.getFileExtension().equals(extension)) {
        return format;
      }
    }
    return null;
  }

  public static HoodieFileFormat getValue(String fileFormat) {
    if (StringUtils.isNullOrEmpty(fileFormat)) {
      return null;
    }
    return HoodieFileFormat.valueOf(fileFormat.toUpperCase());
  }
}
