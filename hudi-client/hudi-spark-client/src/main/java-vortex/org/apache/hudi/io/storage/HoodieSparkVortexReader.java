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

package org.apache.hudi.io.storage;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.storage.StoragePath;

import dev.vortex.api.DataSource;
import dev.vortex.api.Scan;
import dev.vortex.api.ScanOptions;
import dev.vortex.api.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils$;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

/**
 * {@link HoodieSparkFileReader} implementation for Vortex file format, backed by the vortex-jni
 * {@code dev.vortex.api} reader (Session / DataSource / Scan).
 *
 * <p>NOTE: vortex-jni 0.76.0 exposes no file-level custom-metadata API, so the Hudi bloom filter
 * and min/max record-key footers are not available for Vortex files. {@link #readBloomFilter()}
 * returns {@code null} and {@link #readMinMaxRecordKeys()} is unsupported.
 * TODO(#18623): surface bloom/min-max footers once Vortex supports file metadata.
 */
@Slf4j
public class HoodieSparkVortexReader implements HoodieSparkFileReader {
  private final StoragePath path;
  private final String uri;
  private final long dataAllocatorSize;
  // Session/DataSource are held so their native resources are not GC'd while this reader is open;
  // neither is AutoCloseable in vortex-jni 0.76.0 (cleanup is implicit).
  private final Session metadataSession;
  private final DataSource metadataDataSource;
  private final BufferAllocator metadataAllocator;

  public HoodieSparkVortexReader(StoragePath path) {
    this(path, Long.parseLong(HoodieStorageConfig.VORTEX_READ_ALLOCATOR_SIZE_BYTES.defaultValue()));
  }

  public HoodieSparkVortexReader(StoragePath path, long dataAllocatorSize) {
    this.path = path;
    this.uri = toVortexUri(path);
    this.dataAllocatorSize = dataAllocatorSize;
    this.metadataAllocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-meta-" + path.getName(),
        Long.parseLong(HoodieStorageConfig.VORTEX_READ_METADATA_ALLOCATOR_SIZE_BYTES.defaultValue()));
    try {
      this.metadataSession = Session.create();
      this.metadataDataSource = DataSource.open(metadataSession, uri);
    } catch (Exception e) {
      metadataAllocator.close();
      throw new HoodieException("Failed to open Vortex file: " + path, e);
    }
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    throw new UnsupportedOperationException(
        "Min/max record keys are not available for Vortex files (no file-metadata support in vortex-jni 0.76.0)");
  }

  @Override
  public BloomFilter readBloomFilter() {
    // Vortex files carry no Hudi bloom filter footer yet; callers treat null as "no bloom filter".
    return null;
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    Set<Pair<String, Long>> result = new HashSet<>();
    long position = 0;

    try (ClosableIterator<String> keyIterator = getRecordKeyIterator()) {
      while (keyIterator.hasNext()) {
        String recordKey = keyIterator.next();
        if (candidateRowKeys == null || candidateRowKeys.isEmpty()
                || candidateRowKeys.contains(recordKey)) {
          result.add(Pair.of(recordKey, position));
        }
        position++;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to filter row keys from Vortex file: " + path, e);
    }

    return result;
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema) throws IOException {
    return getRecordIterator(requestedSchema);
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(HoodieSchema schema) throws IOException {
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(schema);
    return new CloseableMappingIterator<>(iterator, data -> unsafeCast(new HoodieSparkRecord(data)));
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    HoodieSchema recordKeySchema = HoodieSchemaUtils.getRecordKeySchema();
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(recordKeySchema);
    return new CloseableMappingIterator<>(iterator, data -> data.getUTF8String(0).toString());
  }

  private ClosableIterator<UnsafeRow> getUnsafeRowIterator(HoodieSchema requestedSchema) {
    StructType requestedSparkSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(requestedSchema);

    BufferAllocator allocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-data-" + path.getName(), dataAllocatorSize);

    try {
      Session session = Session.create();
      DataSource dataSource = DataSource.open(session, uri);
      // Read all columns; VortexRecordIterator projects the requested columns by name.
      Scan scan = dataSource.scan(ScanOptions.of());
      return new VortexRecordIterator(allocator, session, dataSource, scan, requestedSparkSchema, uri);
    } catch (Exception e) {
      allocator.close();
      throw new HoodieException("Failed to create Vortex reader for: " + path, e);
    }
  }

  @Override
  public HoodieSchema getSchema() {
    try {
      Schema arrowSchema = metadataDataSource.arrowSchema(metadataAllocator);
      StructType structType = (StructType) ArrowUtils$.MODULE$.fromArrowSchema(arrowSchema);
      return HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(structType, "record", "", false);
    } catch (Exception e) {
      throw new HoodieException("Failed to read schema from Vortex file: " + path, e);
    }
  }

  @Override
  public void close() {
    try {
      metadataAllocator.close();
    } catch (Exception e) {
      log.warn("Error while closing Vortex metadata allocator: {}", e.getMessage());
    }
  }

  @Override
  public long getTotalRecords() {
    try {
      return metadataDataSource.rowCount().asOptional().orElseThrow(
          () -> new HoodieException("Vortex row count unavailable for file: " + path));
    } catch (Exception e) {
      throw new HoodieException("Failed to get row count from Vortex file: " + path, e);
    }
  }

  /**
   * Vortex requires a well-formed URL. Remote-store {@link StoragePath}s already carry a scheme
   * (s3://, hdfs://, ...); local paths may not, so qualify them as {@code file://} URIs.
   */
  private static String toVortexUri(StoragePath path) {
    URI parsed = path.toUri();
    if (parsed.getScheme() == null) {
      return new java.io.File(parsed.getPath()).toURI().toString();
    }
    return parsed.toString();
  }
}
