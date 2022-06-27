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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.table.log.HoodieUnMergedLogRecordScanner;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueProducer;
import org.apache.hudi.common.util.queue.FunctionBasedQueueProducer;
import org.apache.hudi.common.util.queue.IteratorBasedQueueProducer;
import org.apache.hudi.hadoop.RecordReaderValueIterator;
import org.apache.hudi.hadoop.SafeParquetRecordReaderWrapper;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

class RealtimeUnmergedRecordReader extends AbstractRealtimeRecordReader
    implements RecordReader<NullWritable, ArrayWritable> {

  // Log Record unmerged scanner
  private final HoodieUnMergedLogRecordScanner logRecordScanner;

  // Parquet record reader
  private final RecordReader<NullWritable, ArrayWritable> parquetReader;

  // Parquet record iterator wrapper for the above reader
  private final RecordReaderValueIterator<NullWritable, ArrayWritable> parquetRecordsIterator;

  // Executor that runs the above producers in parallel
  private final BoundedInMemoryExecutor<ArrayWritable, ArrayWritable, ?> executor;

  // Iterator for the buffer consumer
  private final Iterator<ArrayWritable> iterator;

  /**
   * Construct a Unmerged record reader that parallely consumes both parquet and log records and buffers for upstream
   * clients to consume.
   *
   * @param split File split
   * @param job Job Configuration
   * @param realReader Parquet Reader
   */
  public RealtimeUnmergedRecordReader(RealtimeSplit split, JobConf job,
      RecordReader<NullWritable, ArrayWritable> realReader) {
    super(split, job);
    this.parquetReader = new SafeParquetRecordReaderWrapper(realReader);
    // Iterator for consuming records from parquet file
    this.parquetRecordsIterator = new RecordReaderValueIterator<>(this.parquetReader);
    this.executor = new BoundedInMemoryExecutor<>(
        HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes(jobConf), getParallelProducers(),
        Option.empty(), Function.identity(), new DefaultSizeEstimator<>(), Functions.noop());
    // Consumer of this record reader
    this.iterator = this.executor.getQueue().iterator();
    this.logRecordScanner = HoodieUnMergedLogRecordScanner.newBuilder()
        .withFileSystem(FSUtils.getFs(split.getPath().toString(), this.jobConf))
        .withBasePath(split.getBasePath())
        .withLogFilePaths(split.getDeltaLogPaths())
        .withReaderSchema(getReaderSchema())
        .withLatestInstantTime(split.getMaxCommitTime())
        .withReadBlocksLazily(Boolean.parseBoolean(this.jobConf.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP, HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED)))
        .withReverseReader(false)
        .withBufferSize(this.jobConf.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP, HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withLogRecordScannerCallback(record -> {
          // convert Hoodie log record to Hadoop AvroWritable and buffer
          GenericRecord rec = (GenericRecord) record.toIndexedRecord(getReaderSchema(), payloadProps).get().getData();
          ArrayWritable aWritable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(rec, getHiveSchema());
          this.executor.getQueue().insertRecord(aWritable);
        })
        .withRecordMerger(HoodieRecordUtils.loadRecordMerger(HoodieAvroRecordMerger.class.getName()))
        .build();
    // Start reading and buffering
    this.executor.startProducers();
  }

  /**
   * Setup log and parquet reading in parallel. Both write to central buffer.
   */
  private List<BoundedInMemoryQueueProducer<ArrayWritable>> getParallelProducers() {
    List<BoundedInMemoryQueueProducer<ArrayWritable>> producers = new ArrayList<>();
    producers.add(new FunctionBasedQueueProducer<>(buffer -> {
      logRecordScanner.scan();
      return null;
    }));
    producers.add(new IteratorBasedQueueProducer<>(parquetRecordsIterator));
    return producers;
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) {
    if (!iterator.hasNext()) {
      return false;
    }
    // Copy from buffer iterator and set to passed writable
    value.set(iterator.next().get());
    return true;
  }

  @Override
  public NullWritable createKey() {
    return parquetReader.createKey();
  }

  @Override
  public ArrayWritable createValue() {
    return parquetReader.createValue();
  }

  @Override
  public long getPos() {
    // TODO: vb - No logical way to represent parallel stream pos in a single long.
    // Should we just return invalid (-1). Where is it used ?
    return 0;
  }

  @Override
  public void close() throws IOException {
    this.parquetRecordsIterator.close();
    this.executor.shutdownNow();
  }

  @Override
  public float getProgress() throws IOException {
    return Math.min(parquetReader.getProgress(), logRecordScanner.getProgress());
  }
}
