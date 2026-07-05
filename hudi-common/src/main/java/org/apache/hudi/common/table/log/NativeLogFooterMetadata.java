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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Shared on-disk contract for RFC-103 native log files. The log block header (schema, instant time,
 * etc.) is persisted as a single JSON entry in the native file footer, keyed by
 * {@link #FOOTER_METADATA_KEY}. This is the single source of truth used by both the write path
 * ({@code HoodieNativeLogFormatWriter}) and the read path ({@code HoodieNativeLogFileReader}) so the
 * two cannot drift apart.
 */
public class NativeLogFooterMetadata {

  /**
   * Footer key under which the serialized log block header is stored in a native log file.
   */
  public static final String FOOTER_METADATA_KEY = "hudi.log.format.metadata";

  private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE =
      new TypeReference<LinkedHashMap<String, String>>() {
      };

  private NativeLogFooterMetadata() {
  }

  /**
   * Serializes the log block header into the footer key/value map written to the native file.
   * The {@link HeaderMetadataType#VERSION} entry is injected automatically.
   */
  public static Map<String, String> toFooterMetadata(Map<HeaderMetadataType, String> header) {
    Map<String, String> logFormatMetadata = new LinkedHashMap<>();
    logFormatMetadata.put(HeaderMetadataType.VERSION.name(), String.valueOf(HoodieLogFormat.CURRENT_VERSION));
    header.forEach((key, value) -> {
      if (value != null) {
        logFormatMetadata.put(key.name(), value);
      }
    });
    String serialized;
    try {
      serialized = JsonUtils.getObjectMapper().writeValueAsString(logFormatMetadata);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to serialize native log footer metadata", e);
    }
    Map<String, String> footer = new LinkedHashMap<>();
    footer.put(FOOTER_METADATA_KEY, serialized);
    return footer;
  }

  /**
   * Parses the log block header back from the native file footer key/value map. Unknown header
   * types are ignored so that files written by newer minor versions remain readable.
   */
  public static Map<HeaderMetadataType, String> fromFooterMetadata(Map<String, String> footerMetadata) {
    Map<HeaderMetadataType, String> header = new LinkedHashMap<>();
    String serialized = footerMetadata.get(FOOTER_METADATA_KEY);
    if (serialized == null) {
      return header;
    }
    Map<String, String> logFormatMetadata;
    try {
      logFormatMetadata = JsonUtils.getObjectMapper().readValue(serialized, MAP_TYPE);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to parse native log footer metadata: " + serialized, e);
    }
    logFormatMetadata.forEach((name, value) -> {
      HeaderMetadataType type = headerTypeOrNull(name);
      if (type != null) {
        header.put(type, value);
      }
    });
    validateFormatVersion(header);
    return header;
  }

  /**
   * Reads the table schema embedded in a native data log file footer.
   *
   * <p>Native data logs persist the synthetic log-block header, including {@link HeaderMetadataType#SCHEMA},
   * in the footer metadata entry keyed by {@link #FOOTER_METADATA_KEY}. This method is intended for
   * schema discovery paths that do not have a {@code HoodieReaderContext} and therefore cannot construct
   * a {@code HoodieNativeLogFileReader}. Native delete logs do not expose a generic table schema through
   * this API because their schema depends on the table schema and configured ordering fields; those files
   * return {@code null}, matching the legacy behavior when no data-block schema can be discovered.
   *
   * @param storage          storage used to read the native log file footer
   * @param path             native log file path
   * @param nativeLogMatcher matcher returned by {@code FSUtils.matchNativeLogFile(path.getName())}
   * @return table schema for native data logs, or {@code null} for native non-data logs
   */
  public static HoodieSchema readSchemaFromNativeLogFile(
      HoodieStorage storage, StoragePath path, Matcher nativeLogMatcher) {
    String nativeLogType = nativeLogMatcher.group(8);
    if (!"log".equals(nativeLogType)) {
      return null;
    }

    HoodieFileFormat fileFormat = HoodieFileFormat.fromFileExtension("." + nativeLogMatcher.group(9));
    Map<String, String> footer = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(fileFormat)
        .readFooter(storage, false, path, FOOTER_METADATA_KEY);
    Map<HeaderMetadataType, String> header = fromFooterMetadata(footer);
    String schema = header.get(HeaderMetadataType.SCHEMA);
    if (StringUtils.isNullOrEmpty(schema)) {
      throw new HoodieIOException("Missing required native log schema metadata '"
          + HeaderMetadataType.SCHEMA.name() + "' in footer key '"
          + FOOTER_METADATA_KEY + "' for " + path);
    }
    return HoodieSchema.parse(schema);
  }

  private static void validateFormatVersion(Map<HeaderMetadataType, String> header) {
    String version = header.get(HeaderMetadataType.VERSION);
    if (version == null) {
      return;
    }
    int parsedVersion;
    try {
      parsedVersion = Integer.parseInt(version.trim());
    } catch (NumberFormatException e) {
      throw new HoodieIOException("Invalid native log format version in footer metadata: " + version);
    }
    if (parsedVersion > HoodieLogFormat.CURRENT_VERSION) {
      throw new HoodieNotSupportedException("Native log format version " + parsedVersion
          + " is newer than the supported version " + HoodieLogFormat.CURRENT_VERSION
          + ". Please upgrade Hudi to read this log file.");
    }
  }

  private static HeaderMetadataType headerTypeOrNull(String name) {
    try {
      return HeaderMetadataType.valueOf(name);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
