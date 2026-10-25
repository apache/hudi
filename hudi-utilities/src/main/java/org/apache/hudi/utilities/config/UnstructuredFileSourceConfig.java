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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.utilities.sources.helpers.unstructured.DocumentParserType;

import javax.annotation.concurrent.Immutable;

import static org.apache.hudi.common.util.ConfigUtils.STREAMER_CONFIG_PREFIX;

/**
 * Unstructured File DFS Source Configs.
 */
@Immutable
@ConfigClassProperty(name = "Unstructured File DFS Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of the unstructured file DFS source "
        + "in Hudi Streamer, which ingests arbitrary files (documents, images, videos) as BLOB "
        + "columns with extracted text, metadata and chunks.")
public class UnstructuredFileSourceConfig extends HoodieConfig {

  private static final String PREFIX = STREAMER_CONFIG_PREFIX + "source.unstructured.";

  public static final ConfigProperty<Long> BLOB_INLINE_MAX_BYTES = ConfigProperty
      .key(PREFIX + "blob.inline.max.bytes")
      .defaultValue(1024L * 1024L)
      .sinceVersion("1.2.0")
      .withDocumentation("Files at or below this size are stored INLINE in the blob column; "
          + "larger files are stored OUT_OF_LINE as a reference to the original file in place. "
          + "This bounds per-row memory: blob bytes above the threshold never enter Spark rows.");

  public static final ConfigProperty<String> DOCUMENT_PARSER = ConfigProperty
      .key(PREFIX + "document.parser")
      .defaultValue("TIKA")
      .sinceVersion("1.2.0")
      .withDocumentation(DocumentParserType.class, "Parser used to extract text and metadata "
          + "from ingested files. TIKA uses Apache Tika with automatic format detection; CUSTOM "
          + "loads the DocumentParser implementation named by " + PREFIX + "parser.class.");

  public static final ConfigProperty<String> PARSER_CLASS = ConfigProperty
      .key(PREFIX + "parser.class")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Fully qualified class name of a custom DocumentParser implementation; "
          + "only read when " + PREFIX + "document.parser is CUSTOM.");

  public static final ConfigProperty<Boolean> PARSE_ENABLED = ConfigProperty
      .key(PREFIX + "parse.enabled")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("When false, files are ingested as blobs only: no text extraction, "
          + "metadata or chunking is performed.");

  public static final ConfigProperty<Long> PARSE_MAX_BYTES = ConfigProperty
      .key(PREFIX + "parse.max.bytes")
      .defaultValue(128L * 1024L * 1024L)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Files larger than this are not parsed (parse_status=SKIPPED); they are "
          + "still ingested as blobs. Bounds parser memory on pathological inputs.");

  public static final ConfigProperty<Integer> PARSE_MAX_TEXT_CHARS = ConfigProperty
      .key(PREFIX + "parse.max.text.chars")
      .defaultValue(1_000_000)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Extracted text is capped at this many characters "
          + "(parse_status=TRUNCATED when the cap is hit).");

  public static final ConfigProperty<String> FILE_EXTENSIONS = ConfigProperty
      .key(PREFIX + "file.extensions")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Optional comma-separated allowlist of file extensions to ingest "
          + "(e.g. 'pdf,docx,html'). Empty ingests every file under the source root except "
          + "those matching " + PREFIX + "file.extensions.ignore. When set, the allowlist "
          + "alone decides.");

  public static final ConfigProperty<String> FILE_EXTENSIONS_IGNORE = ConfigProperty
      .key(PREFIX + "file.extensions.ignore")
      .defaultValue("parquet,orc,avro,hfile")
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Comma-separated denylist of file extensions skipped when no allowlist "
          + "is configured. Defaults to columnar/data file formats, which belong to structured "
          + "sources: directories mixing data files and documents ingest only the documents.");

  public static final ConfigProperty<Integer> CHUNK_SIZE_CHARS = ConfigProperty
      .key(PREFIX + "chunk.size.chars")
      .defaultValue(2000)
      .sinceVersion("1.2.0")
      .withDocumentation("Size in characters of each text chunk emitted in the chunks column.");

  public static final ConfigProperty<Integer> CHUNK_OVERLAP_CHARS = ConfigProperty
      .key(PREFIX + "chunk.overlap.chars")
      .defaultValue(200)
      .sinceVersion("1.2.0")
      .withDocumentation("Number of characters consecutive chunks overlap by.");

  public static final ConfigProperty<Integer> LISTING_PARALLELISM = ConfigProperty
      .key(PREFIX + "listing.parallelism")
      .defaultValue(0)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Number of Spark partitions used to stat, fetch and parse the files "
          + "selected in one batch. The default 0 sizes to the cluster "
          + "(spark default parallelism, i.e. total executor cores); set explicitly to cap "
          + "parse/embedding concurrency.");
}
