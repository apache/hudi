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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.CachingIterator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * A file group reader that iterates through the records in a single file group.
 * <p>
 * This should be used by the every engine integration, by plugging in a
 * {@link HoodieReaderContext<T>} implementation.
 *
 * @param <T> The type of engine-specific record representation, e.g.,{@code InternalRow}
 *            in Spark and {@code RowData} in Flink.
 */
public final class HoodieNewFileGroupReader<T> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieNewFileGroupReader.class);
  private final HoodieReaderContext<T> readerContext;
  private final HoodieStorage storage;
  private final QueryType queryType;
  private final TypedProperties props;
  private final ClosableIterator<T> recordIterator;

  public HoodieNewFileGroupReader(
      HoodieReaderContext<T> readerContext,
      QueryType queryType,
      HoodieStorage storage,
      TypedProperties props,
      String tablePath,
      String latestCommitTime,
      List<FileSlice> fileSlices,
      Schema dataSchema,
      Schema requestedSchema,
      Option<InternalSchema> internalSchemaOpt,
      HoodieTableMetaClient hoodieTableMetaClient,
      boolean shouldUseRecordPosition) {
    this.readerContext = readerContext;
    this.storage = storage;
    this.queryType = queryType;
    this.props = props;

    switch (this.queryType) {
      case SNAPSHOT:
      case TIME_TRAVEL:
      case READ_OPTIMIZED:
      case INCREMENTAL:
        Optional<FileSlice> selectedFileSlice = getLatestFileSlice(fileSlices, latestCommitTime);
        if (!selectedFileSlice.isPresent()) {
          throw new IllegalArgumentException("Cannot find a file slice to read");
        }
        HoodieFileGroupReader<T> fileGroupReader = new HoodieFileGroupReader<>(
            readerContext,
            storage,
            tablePath,
            latestCommitTime,
            selectedFileSlice.get(),
            dataSchema,
            requestedSchema,
            internalSchemaOpt,
            hoodieTableMetaClient,
            props,
            0,
            -1,
            shouldUseRecordPosition);
        // TODO: refactor the iterator to simplify the logic here.
        recordIterator = new HoodieFileGroupReader.HoodieFileGroupReaderIterator<>(fileGroupReader);
        break;
      case CDC:
        recordIterator = new HoodieCDCFileGroupReaderIterator<>(
            readerContext,
            storage,
            props,
            tablePath,
            latestCommitTime,
            fileSlices,
            dataSchema,
            requestedSchema,
            internalSchemaOpt,
            hoodieTableMetaClient,
            shouldUseRecordPosition);
        break;
      default:
        throw new IllegalArgumentException(String.format(
            "Unrecognized query type: %s",
            this.queryType.name()));
    }
  }

  // Find the correct file slice to work on.
  private Optional<FileSlice> getLatestFileSlice(List<FileSlice> fileSlices,
                                                 String latestCommitTime) {
    return fileSlices
        .stream()
        .filter(f -> f.getLatestInstantTime().equals(latestCommitTime))
        .findFirst();
  }

  @Override
  public void close() {
    recordIterator.close();
  }

  public enum QueryType {
    SNAPSHOT,
    INCREMENTAL,
    CDC,
    READ_OPTIMIZED,
    TIME_TRAVEL
  }
}
