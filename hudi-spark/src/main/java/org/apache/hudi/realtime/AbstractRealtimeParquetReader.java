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

package org.apache.hudi.realtime;

import org.apache.hudi.common.table.log.LogReaderUtils;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.UnmaterializableRecordCounter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.hadoop.ParquetInputFormat.RECORD_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.STRICT_TYPE_CHECKING;

/**
 * This class is customized from org.apache.parquet.hadoop.InternalParquetRecordReader combining with AbstractRealtimeRecordReader.
 * @param <T>
 */
abstract class AbstractRealtimeParquetReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRealtimeParquetReader.class);
  ////////////////////////From Internal parquet record reader
  protected final Filter filter;
  protected MessageType requestedSchema;
  protected MessageType fileSchema;
  protected MessageType logSchema;
  protected int columnCount;
  protected final ReadSupport<T> readSupport;
  protected RecordMaterializer<T> recordConverter;
  protected T currentValue;
  protected long total;
  protected long current = 0;
  protected int currentBlock = -1;
  protected ParquetFileReader reader;
  protected org.apache.parquet.io.RecordReader<T> recordReader;
  protected boolean strictTypeChecking;
  protected long totalTimeSpentReadingBytes;
  protected long totalTimeSpentProcessingRecords;
  protected long startedAssemblingCurrentBlockAt;
  protected UnmaterializableRecordCounter unmaterializableRecordCounter;

  protected boolean filterRecords = true;
  protected ColumnIOFactory columnIOFactory = null;
  protected long totalCountLoadedSoFar = 0;
  ///////////////////////////From Hoodie Abstract realtime record reader
  protected final HoodieRealtimeFileSplit split;
  protected final JobConf jobConf;
  protected final boolean usesCustomPayload;

  public Schema logAvroSchema;
  //////////////////////

  public AbstractRealtimeParquetReader(ReadSupport<T> readSupport, Filter filter, HoodieRealtimeFileSplit split, JobConf job) {
    this.readSupport = readSupport;
    this.filter = checkNotNull(filter, "filter");
    this.split = split;
    this.jobConf = job;
    //TODO: Support custom payload
    this.usesCustomPayload = false;
  }

  public AbstractRealtimeParquetReader(ReadSupport<T> readSupport, HoodieRealtimeFileSplit split, JobConf job) {
    this(readSupport, FilterCompat.NOOP, split, job);
  }

  protected void checkRead() throws IOException {
    if (current == totalCountLoadedSoFar) {
      if (current != 0) {
        totalTimeSpentProcessingRecords += (System.currentTimeMillis() - startedAssemblingCurrentBlockAt);
        if (LOG.isInfoEnabled()) {
          LOG.info("Assembled and processed "
              + totalCountLoadedSoFar + " records from " + columnCount
              + " columns in " + totalTimeSpentProcessingRecords + " ms: "
              + ((float)totalCountLoadedSoFar / totalTimeSpentProcessingRecords) + " rec/ms, "
              + ((float)totalCountLoadedSoFar * columnCount / totalTimeSpentProcessingRecords) + " cell/ms");
          final long totalTime = totalTimeSpentProcessingRecords + totalTimeSpentReadingBytes;
          if (totalTime != 0) {
            final long percentReading = 100 * totalTimeSpentReadingBytes / totalTime;
            final long percentProcessing = 100 * totalTimeSpentProcessingRecords / totalTime;
            LOG.info("time spent so far " + percentReading + "% reading (" + totalTimeSpentReadingBytes + " ms) and "
                + percentProcessing + "% processing (" + totalTimeSpentProcessingRecords + " ms)");
          }
        }
      }

      LOG.info("at row " + current + ". reading next block");
      long t0 = System.currentTimeMillis();
      PageReadStore pages = reader.readNextRowGroup();
      if (pages == null) {
        throw new IOException("expecting more rows but reached last block. Read " + current + " out of " + total);
      }
      long timeSpentReading = System.currentTimeMillis() - t0;
      totalTimeSpentReadingBytes += timeSpentReading;
      BenchmarkCounter.incrementTime(timeSpentReading);
      if (LOG.isInfoEnabled()) {
        LOG.info("block read in memory in {} ms. row count = {}", timeSpentReading, pages.getRowCount());
      }
      LOG.debug("initializing Record assembly with requested schema {}", requestedSchema);
      MessageColumnIO columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);
      recordReader = columnIO.getRecordReader(pages, recordConverter,
          filterRecords ? filter : FilterCompat.NOOP);
      startedAssemblingCurrentBlockAt = System.currentTimeMillis();
      totalCountLoadedSoFar += pages.getRowCount();
      ++ currentBlock;
    }
  }

  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  public T getCurrentValue() throws IOException,
      InterruptedException {
    return currentValue;
  }

  public float getProgress() throws IOException, InterruptedException {
    return (float) current / total;
  }

  // TODO: Remove this?
  public void initialize(ParquetFileReader reader, ParquetReadOptions options) throws IOException {
    // copy custom configuration to the Configuration passed to the ReadSupport
    Configuration conf = new Configuration();
    if (options instanceof HadoopReadOptions) {
      conf = ((HadoopReadOptions) options).getConf();
    }
    for (String property : options.getPropertyNames()) {
      conf.set(property, options.getProperty(property));
    }

    // initialize a ReadContext for this file
    this.reader = reader;
    FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
    this.fileSchema = parquetFileMetadata.getSchema();
    Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
    ReadSupport.ReadContext readContext = readSupport.init(new InitContext(conf, toSetMultiMap(fileMetadata), fileSchema));
    this.columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());
    this.requestedSchema = readContext.getRequestedSchema();
    this.columnCount = requestedSchema.getPaths().size();
    this.recordConverter = readSupport.prepareForRead(conf, fileMetadata, fileSchema, readContext);
    this.strictTypeChecking = options.isEnabled(STRICT_TYPE_CHECKING, true);
    this.total = reader.getRecordCount();
    this.unmaterializableRecordCounter = new UnmaterializableRecordCounter(options, total);
    this.filterRecords = options.useRecordFilter();
    reader.setRequestedSchema(requestedSchema);
    LOG.info("RecordReader initialized will read a total of {} records.", total);
    // init() from hoodie side
    this.logAvroSchema =
        LogReaderUtils.readLatestSchemaFromLogFiles(split.getBasePath(), split.getDeltaLogPaths(), jobConf);
    AvroSchemaConverter schemaConverter = new AvroSchemaConverter();
    if (this.logAvroSchema == null) {
      this.logSchema = fileSchema;
      this.logAvroSchema = schemaConverter.convert(fileSchema);
      LOG.info("No Avro schema found, use the parquet file schema instead: " + this.logAvroSchema.toString());
    } else {
      this.logSchema = schemaConverter.convert(this.logAvroSchema);
    }
  }

  public void initialize(ParquetFileReader reader, Configuration configuration)
      throws IOException {
    // initialize a ReadContext for this file
    this.reader = reader;
    FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
    this.fileSchema = parquetFileMetadata.getSchema();
    Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
    ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
        configuration, toSetMultiMap(fileMetadata), fileSchema));
    this.columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());
    this.requestedSchema = readContext.getRequestedSchema();
    this.columnCount = requestedSchema.getPaths().size();
    this.recordConverter = readSupport.prepareForRead(
        configuration, fileMetadata, fileSchema, readContext);
    this.strictTypeChecking = configuration.getBoolean(STRICT_TYPE_CHECKING, true);
    this.total = reader.getRecordCount();
    this.unmaterializableRecordCounter = new UnmaterializableRecordCounter(configuration, total);
    this.filterRecords = configuration.getBoolean(RECORD_FILTERING_ENABLED, true);
    reader.setRequestedSchema(requestedSchema);
    Log.info(this.fileSchema.toString());
    LOG.info("RecordReader initialized will read a total of {} records.", total);
    // init() from hoodie side
    this.logAvroSchema =
        LogReaderUtils.readLatestSchemaFromLogFiles(split.getBasePath(), split.getDeltaLogPaths(), jobConf);
    AvroSchemaConverter schemaConverter = new AvroSchemaConverter();
    if (this.logAvroSchema == null) {
      this.logSchema = fileSchema;
      this.logAvroSchema = schemaConverter.convert(fileSchema);
      LOG.warn("null" + this.logAvroSchema.toString());
    } else {
      this.logSchema = schemaConverter.convert(this.logAvroSchema);
    }
  }

  public abstract boolean nextKeyValue() throws IOException, InterruptedException;

  private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
    Map<K, Set<V>> setMultiMap = new HashMap<K, Set<V>>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      Set<V> set = new HashSet<V>();
      set.add(entry.getValue());
      setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
    }
    return Collections.unmodifiableMap(setMultiMap);
  }

  public Schema getLogAvroSchema() {
    return logAvroSchema;
  }

  public long getMaxCompactionMemoryInBytes() {
    // jobConf.getMemoryForMapTask() returns in MB
    return (long) Math
        .ceil(Double.parseDouble(jobConf.get(HoodieRealtimeConfig.COMPACTION_MEMORY_FRACTION_PROP,
            HoodieRealtimeConfig.DEFAULT_COMPACTION_MEMORY_FRACTION))
            * jobConf.getMemoryForMapTask() * 1024 * 1024L);
  }
}
