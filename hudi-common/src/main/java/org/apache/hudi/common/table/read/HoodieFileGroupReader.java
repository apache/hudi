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

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.CachingIterator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.findNestedField;
import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;

/**
 * A file group reader that iterates through the records in a single file group.
 * <p>
 * This should be used by the every engine integration, by plugging in a
 * {@link HoodieReaderContext<T>} implementation.
 *
 * @param <T> The type of engine-specific record representation, e.g.,{@code InternalRow}
 *            in Spark and {@code RowData} in Flink.
 */
public final class HoodieFileGroupReader<T> implements Closeable {
  private final HoodieReaderContext<T> readerContext;
  private final Option<HoodieBaseFile> hoodieBaseFileOption;
  private final List<HoodieLogFile> logFiles;
  private final Configuration hadoopConf;
  private final TypedProperties props;
  // Byte offset to start reading from the base file
  private final long start;
  // Length of bytes to read from the base file
  private final long length;
  // Core structure to store and process records.
  private final HoodieFileGroupRecordBuffer<T> recordBuffer;
  private final HoodieFileGroupReaderState readerState = new HoodieFileGroupReaderState();
  private ClosableIterator<T> baseFileIterator;
  private HoodieRecordMerger recordMerger;

  private final Schema dataSchema;

  // requestedSchema: the schema that the caller requests
  private final Schema requestedSchema;

  // requiredSchema: the requestedSchema with any additional columns required for merging etc
  private final Schema requiredSchema;

  private final HoodieTableConfig hoodieTableConfig;

  private final Option<UnaryOperator<T>> outputConverter;

  public HoodieFileGroupReader(HoodieReaderContext<T> readerContext,
                               Configuration hadoopConf,
                               String tablePath,
                               String latestCommitTime,
                               FileSlice fileSlice,
                               Schema dataSchema,
                               Schema requestedSchema,
                               TypedProperties props,
                               HoodieTableConfig tableConfig,
                               long start,
                               long length,
                               boolean shouldUseRecordPosition,
                               long maxMemorySizeInBytes,
                               String spillableMapBasePath,
                               ExternalSpillableMap.DiskMapType diskMapType,
                               boolean isBitCaskDiskMapCompressionEnabled) {
    this.readerContext = readerContext;
    this.hadoopConf = hadoopConf;
    this.hoodieBaseFileOption = fileSlice.getBaseFile();
    this.logFiles = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
    this.props = props;
    this.start = start;
    this.length = length;
    this.recordMerger = readerContext.getRecordMerger(tableConfig.getRecordMergerStrategy());
    this.readerState.tablePath = tablePath;
    this.readerState.latestCommitTime = latestCommitTime;
    this.dataSchema = dataSchema;
    this.requestedSchema = requestedSchema;
    this.hoodieTableConfig = tableConfig;
    this.requiredSchema = generateRequiredSchema();
    if (!requestedSchema.equals(requiredSchema)) {
      this.outputConverter = Option.of(readerContext.projectRecord(requiredSchema, requestedSchema));
    } else {
      this.outputConverter = Option.empty();
    }
    this.readerState.baseFileAvroSchema = requiredSchema;
    this.readerState.logRecordAvroSchema = requiredSchema;
    this.readerState.mergeProps.putAll(props);
    this.recordBuffer = this.logFiles.isEmpty()
        ? null
        : shouldUseRecordPosition
        ? new HoodiePositionBasedFileGroupRecordBuffer<>(
        readerContext, requiredSchema, requiredSchema, Option.empty(), Option.empty(),
        recordMerger, props, maxMemorySizeInBytes, spillableMapBasePath, diskMapType, isBitCaskDiskMapCompressionEnabled)
        : new HoodieKeyBasedFileGroupRecordBuffer<>(
        readerContext, requiredSchema, requiredSchema, Option.empty(), Option.empty(),
        recordMerger, props, maxMemorySizeInBytes, spillableMapBasePath, diskMapType, isBitCaskDiskMapCompressionEnabled);
  }

  /**
   * Initialize internal iterators on the base and log files.
   */
  public void initRecordIterators() throws IOException {
    ClosableIterator<T> iter = makeBaseFileIterator();
    if (logFiles.isEmpty()) {
      this.baseFileIterator = CachingIterator.wrap(iter, readerContext);
    } else {
      this.baseFileIterator = iter;
      scanLogFiles();
      recordBuffer.setBaseFileIterator(baseFileIterator);
    }
  }

  private ClosableIterator<T> makeBaseFileIterator() throws IOException {
    if (!hoodieBaseFileOption.isPresent()) {
      return new EmptyIterator<>();
    }

    HoodieBaseFile baseFile = hoodieBaseFileOption.get();
    if (baseFile.getBootstrapBaseFile().isPresent()) {
      return makeBootstrapBaseFileIterator(baseFile);
    }

    return readerContext.getFileRecordIterator(baseFile.getHadoopPath(), start, length,
         dataSchema, requiredSchema, hadoopConf);
  }

  private Schema generateRequiredSchema() {
    //might need to change this if other queries than mor have mandatory fields
    if (logFiles.isEmpty()) {
      return requestedSchema;
    }

    List<Schema.Field> addedFields = new ArrayList<>();
    for (String field : recordMerger.getMandatoryFieldsForMerging(hoodieTableConfig)) {
      if (requestedSchema.getField(field) == null) {
        Option<Schema.Field> foundFieldOpt  = findNestedField(dataSchema, field);
        if (!foundFieldOpt.isPresent()) {
          throw new IllegalArgumentException("Field: " + field + " does not exist in the table schema");
        }
        Schema.Field foundField = foundFieldOpt.get();
        addedFields.add(foundField);
      }
    }

    if (addedFields.isEmpty()) {
      return maybeReorderForBootstrap(requestedSchema);
    }

    return maybeReorderForBootstrap(appendFieldsToSchema(requestedSchema, addedFields));
  }

  private Schema maybeReorderForBootstrap(Schema input) {
    if (this.hoodieBaseFileOption.isPresent() && this.hoodieBaseFileOption.get().getBootstrapBaseFile().isPresent()) {
      Pair<List<Schema.Field>, List<Schema.Field>> requiredFields = getDataAndMetaCols(input);
      if (!(requiredFields.getLeft().isEmpty() || requiredFields.getRight().isEmpty())) {
        return createSchemaFromFields(Stream.concat(requiredFields.getLeft().stream(), requiredFields.getRight().stream())
            .collect(Collectors.toList()));
      }
    }
    return input;
  }

  private static Pair<List<Schema.Field>, List<Schema.Field>> getDataAndMetaCols(Schema schema) {
    Map<Boolean, List<Schema.Field>> fieldsByMeta = schema.getFields().stream()
        .collect(Collectors.partitioningBy(f -> HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(f.name())));
    return Pair.of(fieldsByMeta.getOrDefault(true, Collections.emptyList()),
        fieldsByMeta.getOrDefault(false, Collections.emptyList()));
  }

  private Schema createSchemaFromFields(List<Schema.Field> fields) {
    //fields have positions set, so we need to remove them due to avro setFields implementation
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field curr = fields.get(i);
      fields.set(i, new Schema.Field(curr.name(), curr.schema(), curr.doc(), curr.defaultVal()));
    }
    Schema newSchema = Schema.createRecord(dataSchema.getName(), dataSchema.getDoc(), dataSchema.getNamespace(), dataSchema.isError());
    newSchema.setFields(fields);
    return newSchema;
  }

  private ClosableIterator<T> makeBootstrapBaseFileIterator(HoodieBaseFile baseFile) throws IOException {
    BaseFile dataFile = baseFile.getBootstrapBaseFile().get();
    Pair<List<Schema.Field>,List<Schema.Field>> requiredFields = getDataAndMetaCols(requiredSchema);
    Pair<List<Schema.Field>,List<Schema.Field>> allFields = getDataAndMetaCols(dataSchema);

    Option<ClosableIterator<T>> dataFileIterator = requiredFields.getRight().isEmpty() ? Option.empty() :
        Option.of(readerContext.getFileRecordIterator(dataFile.getHadoopPath(), 0, dataFile.getFileLen(),
            createSchemaFromFields(allFields.getRight()), createSchemaFromFields(requiredFields.getRight()), hadoopConf));

    Option<ClosableIterator<T>> skeletonFileIterator = requiredFields.getLeft().isEmpty() ? Option.empty() :
        Option.of(readerContext.getFileRecordIterator(baseFile.getHadoopPath(), 0, baseFile.getFileLen(),
            createSchemaFromFields(allFields.getLeft()), createSchemaFromFields(requiredFields.getLeft()), hadoopConf));
    if (!dataFileIterator.isPresent() && !skeletonFileIterator.isPresent()) {
      throw new IllegalStateException("should not be here if only partition cols are required");
    } else if (!dataFileIterator.isPresent()) {
      return skeletonFileIterator.get();
    } else if (!skeletonFileIterator.isPresent()) {
      return  dataFileIterator.get();
    } else {
      return readerContext.mergeBootstrapReaders(skeletonFileIterator.get(), dataFileIterator.get());
    }
  }

  /**
   * @return {@code true} if the next record exists; {@code false} otherwise.
   * @throws IOException on reader error.
   */
  public boolean hasNext() throws IOException {
    if (recordBuffer == null) {
      return baseFileIterator.hasNext();
    } else {
      return recordBuffer.hasNext();
    }
  }

  /**
   * @return The next record after calling {@link #hasNext}.
   */
  public T next() {
    T nextVal = recordBuffer == null ? baseFileIterator.next() : recordBuffer.next();
    if (outputConverter.isPresent()) {
      return outputConverter.get().apply(nextVal);
    }
    return nextVal;
  }

  private void scanLogFiles() {
    String path = readerState.tablePath;
    FileSystem fs = readerContext.getFs(path, hadoopConf);

    HoodieMergedLogRecordReader logRecordReader = HoodieMergedLogRecordReader.newBuilder()
        .withHoodieReaderContext(readerContext)
        .withFileSystem(fs)
        .withBasePath(readerState.tablePath)
        .withLogFiles(logFiles)
        .withLatestInstantTime(readerState.latestCommitTime)
        .withReaderSchema(readerState.logRecordAvroSchema)
        .withReverseReader(false)
        .withBufferSize(getIntWithAltKeys(props, HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE))
        .withPartition(getRelativePartitionPath(
            new Path(readerState.tablePath), logFiles.get(0).getPath().getParent()))
        .withRecordMerger(recordMerger)
        .withRecordBuffer(recordBuffer)
        .build();
    logRecordReader.close();
  }

  @Override
  public void close() throws IOException {
    if (baseFileIterator != null) {
      baseFileIterator.close();
    }
    if (recordBuffer != null) {
      recordBuffer.close();
    }
  }

  public HoodieFileGroupReaderIterator<T> getClosableIterator() {
    return new HoodieFileGroupReaderIterator<>(this);
  }

  public static class HoodieFileGroupReaderIterator<T> implements ClosableIterator<T> {
    private HoodieFileGroupReader<T> reader;

    public HoodieFileGroupReaderIterator(HoodieFileGroupReader<T> reader) {
      this.reader = reader;
    }

    @Override
    public boolean hasNext() {
      try {
        return reader.hasNext();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read record", e);
      }
    }

    @Override
    public T next() {
      return reader.next();
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to close the reader", e);
      } finally {
        this.reader = null;
      }
    }
  }
}
