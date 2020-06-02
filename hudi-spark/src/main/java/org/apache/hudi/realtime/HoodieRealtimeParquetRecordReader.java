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

import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.parquet.hadoop.ParquetInputFormat.SPLIT_FILES;

/**
 * Custom implementation of org.apache.parquet.hadoop.ParquetRecordReader.
 * This class is a wrapper class. The real reader is the internalReader.
 *
 * @see ParquetRecordReader
 *
 * @param <T> type of the materialized records
 */
public class HoodieRealtimeParquetRecordReader<T> extends RecordReader<Void, T> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieRealtimeParquetRecordReader.class);
  public final CompactedRealtimeParquetReader<T> internalReader;

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   */
  public HoodieRealtimeParquetRecordReader(ReadSupport<T> readSupport, HoodieRealtimeFileSplit split, JobConf job)
      throws IOException {
    this(readSupport, FilterCompat.NOOP, split, job);
  }

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   * @param filter for filtering individual records
   */
  public HoodieRealtimeParquetRecordReader(ReadSupport<T> readSupport, Filter filter, HoodieRealtimeFileSplit split, JobConf job)
      throws IOException {
    this.internalReader = new CompactedRealtimeParquetReader<T>(readSupport, filter, split, job);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    internalReader.close();
  }

  /**
   * always returns null.
   */
  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T getCurrentValue() throws IOException,
      InterruptedException {
    return internalReader.getCurrentValue();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return internalReader.getProgress();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {

    if (ContextUtil.hasCounterMethod(context)) {
      BenchmarkCounter.initCounterFromContext(context);
    } else {
      LOG.error(
          String.format("Can not initialize counter because the class '%s' does not have a '.getCounterMethod'",
              context.getClass().getCanonicalName()));
    }

    initializeInternalReader((HoodieRealtimeFileSplit) inputSplit, ContextUtil.getConfiguration(context));
  }

  public void initialize(InputSplit inputSplit, Configuration configuration, Reporter reporter)
      throws IOException, InterruptedException {
    BenchmarkCounter.initCounterFromReporter(reporter,configuration);
    initializeInternalReader((HoodieRealtimeFileSplit) inputSplit, configuration);
  }

  private void initializeInternalReader(HoodieRealtimeFileSplit split, Configuration configuration) throws IOException {
    Path path = split.getPath();
    ParquetReadOptions.Builder optionsBuilder = HadoopReadOptions.builder(configuration);
    optionsBuilder.withRange(split.getStart(), split.getStart() + split.getLength());

    // open a reader with the metadata filter
    ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(path, configuration), optionsBuilder.build());
    if (!reader.getRowGroups().isEmpty()) {
      checkDeltaByteArrayProblem(
          reader.getFooter().getFileMetaData(), configuration,
          reader.getRowGroups().get(0));
    }

    internalReader.initialize(reader, configuration);
  }

  private void checkDeltaByteArrayProblem(FileMetaData meta, Configuration conf, BlockMetaData block) {
    if (conf.getBoolean(SPLIT_FILES, true)) {
      // this is okay if not using DELTA_BYTE_ARRAY with the bug
      Set<Encoding> encodings = new HashSet<Encoding>();
      for (ColumnChunkMetaData column : block.getColumns()) {
        encodings.addAll(column.getEncodings());
      }
      for (Encoding encoding : encodings) {
        if (CorruptDeltaByteArrays.requiresSequentialReads(meta.getCreatedBy(), encoding)) {
          throw new ParquetDecodingException("Cannot read data due to "
              + "PARQUET-246: to read safely, set " + SPLIT_FILES + " to false");
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return internalReader.nextKeyValue();
  }

}
