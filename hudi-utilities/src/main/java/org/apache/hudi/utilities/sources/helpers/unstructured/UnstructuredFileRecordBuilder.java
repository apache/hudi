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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.BLOB_INLINE_MAX_BYTES;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.CHUNK_OVERLAP_CHARS;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.CHUNK_SIZE_CHARS;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.PARSE_ENABLED;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.PARSE_MAX_BYTES;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.PARSE_MAX_TEXT_CHARS;

/**
 * Executor-side logic turning one file into one source {@link Row}: decides inline vs
 * out-of-line blob placement by size, fetches bytes only for inline files, parses and
 * chunks content. Blob bytes above the inline threshold never enter Spark rows — the
 * blob column carries a reference to the original file in place.
 */
public class UnstructuredFileRecordBuilder implements Serializable {

  private static final long serialVersionUID = 1L;

  private final long inlineMaxBytes;
  private final boolean parseEnabled;
  private final long parseMaxBytes;
  private final int parseMaxTextChars;
  private final String parserClass;
  private final TextChunker chunker;
  private final TypedProperties props;

  private transient DocumentParser parser;

  public UnstructuredFileRecordBuilder(TypedProperties props) {
    this.props = props;
    this.inlineMaxBytes = getLongWithAltKeys(props, BLOB_INLINE_MAX_BYTES);
    // readFully narrows the size to int, so a threshold past Integer.MAX_VALUE would
    // surface as a NegativeArraySizeException on the first oversized file instead
    ValidationUtils.checkArgument(inlineMaxBytes >= 0 && inlineMaxBytes <= Integer.MAX_VALUE,
        BLOB_INLINE_MAX_BYTES.key() + " must be between 0 and " + Integer.MAX_VALUE
            + ", got " + inlineMaxBytes);
    this.parseEnabled = getBooleanWithAltKeys(props, PARSE_ENABLED);
    this.parseMaxBytes = getLongWithAltKeys(props, PARSE_MAX_BYTES);
    this.parseMaxTextChars = getIntWithAltKeys(props, PARSE_MAX_TEXT_CHARS);
    this.parserClass = DocumentParserType.resolveParserClass(props);
    this.chunker = new TextChunker(getIntWithAltKeys(props, CHUNK_SIZE_CHARS),
        getIntWithAltKeys(props, CHUNK_OVERLAP_CHARS));
  }

  public Row buildRow(FileSystem fs, UnstructuredFilePathSelector.FileEntry entry) throws IOException {
    String pathStr = entry.path;
    Path path = new Path(pathStr);
    // size and modification time come from the driver's listing; re-statting here would be a
    // second full round of metadata requests against the object store
    long size = entry.size;
    String fileName = path.getName();

    byte[] inlineBytes = null;
    Row blob;
    if (size <= inlineMaxBytes) {
      inlineBytes = readFully(fs, path, (int) size);
      blob = RowFactory.create(HoodieSchema.Blob.INLINE, inlineBytes, null);
    } else {
      Row reference = RowFactory.create(pathStr, null, null, false);
      blob = RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null, reference);
    }

    ParseResult parseResult = parse(fs, path, fileName, size, inlineBytes);
    List<Row> chunks = chunker.chunk(parseResult.getText()).stream()
        .map(c -> RowFactory.create(c.chunkId, c.text, c.charStart))
        .collect(Collectors.toList());

    return RowFactory.create(
        pathStr,
        fileName,
        extensionOf(fileName),
        size,
        entry.modificationTime,
        blob,
        parseResult.getText(),
        // Spark's Row encoder requires a scala Map as the external type for MapType
        scala.collection.JavaConverters.mapAsScalaMapConverter(parseResult.getMetadata()).asScala(),
        chunks.toArray(new Row[0]),
        parseResult.getStatus().name(),
        parseResult.getError());
  }

  private ParseResult parse(FileSystem fs, Path path, String fileName, long size, byte[] inlineBytes) {
    if (!parseEnabled) {
      return ParseResult.skipped("parsing disabled");
    }
    if (size > parseMaxBytes) {
      return ParseResult.skipped("file size " + size + " exceeds " + PARSE_MAX_BYTES.key());
    }
    if (parser == null) {
      parser = (DocumentParser) ReflectionUtils.loadClass(parserClass);
      parser.init(props);
    }
    try (InputStream in = inlineBytes != null
        ? new ByteArrayInputStream(inlineBytes) : fs.open(path)) {
      return parser.parse(in, fileName, parseMaxTextChars);
    } catch (Exception e) {
      return ParseResult.failed(e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  private static byte[] readFully(FileSystem fs, Path path, int size) throws IOException {
    // On-heap by design: Spark's BinaryType external type is byte[], so an off-heap read
    // would still copy onto the heap to build the row. Peak per-task allocation is bounded
    // by BLOB_INLINE_MAX_BYTES (rows stream one at a time); raising that threshold raises
    // task memory accordingly.
    byte[] bytes = new byte[size];
    try (FSDataInputStream in = fs.open(path)) {
      in.readFully(0, bytes);
    }
    return bytes;
  }

  public static String extensionOf(String fileName) {
    int dot = fileName.lastIndexOf('.');
    return dot > 0 && dot < fileName.length() - 1 ? fileName.substring(dot + 1).toLowerCase() : "";
  }
}
