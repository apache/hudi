/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils.source;

import org.apache.hudi.adapter.DataStreamScanProviderAdapter;
import org.apache.hudi.adapter.SourceFunctionAdapter;
import org.apache.hudi.util.JsonDeserializationFunction;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.utils.factory.ContinuousFileSourceFactory.CHECKPOINTS;

/**
 * A continuous file source that can trigger checkpoints continuously.
 *
 * <p>It loads the data in the specified file and split the data into number of checkpoints batches.
 * Say, if you want 4 checkpoints and there are 8 records in the file, the emission strategy is:
 *
 * <pre>
 *   | 2 records | 2 records | 2 records | 2 records |
 *   | cp1       | cp2       |cp3        | cp4       |
 * </pre>
 *
 * <p>If all the data are flushed out, it waits for the next checkpoint to finish and tear down the source.
 *
 * <p>NOTE: this class is represented twice: in test utils and in quickstart. Don't forget to update both files.
 */
public class ContinuousFileSource implements ScanTableSource {

  private final ResolvedSchema tableSchema;
  private final Path path;
  private final Configuration conf;

  public ContinuousFileSource(
      ResolvedSchema tableSchema,
      Path path,
      Configuration conf) {
    this.tableSchema = tableSchema;
    this.path = path;
    this.conf = conf;
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return new DataStreamScanProviderAdapter() {

      @Override
      public boolean isBounded() {
        return false;
      }

      @Override
      public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
        final RowType rowType = (RowType) tableSchema.toSourceRowDataType().getLogicalType();
        return execEnv.addSource(new BoundedSourceFunction(path, conf.get(CHECKPOINTS)))
            .name("continuous_file_source")
            .setParallelism(1)
            .map(JsonDeserializationFunction.getInstance(rowType), InternalTypeInfo.of(rowType));
      }
    };
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public DynamicTableSource copy() {
    return new ContinuousFileSource(this.tableSchema, this.path, this.conf);
  }

  @Override
  public String asSummaryString() {
    return "ContinuousFileSource";
  }

  /**
   * Source function that partition the data into given number checkpoints batches.
   */
  public static class BoundedSourceFunction implements SourceFunctionAdapter<String>, CheckpointListener {
    private final Path path;
    private List<String> dataBuffer;

    private final int checkpoints;
    private final AtomicInteger currentCP = new AtomicInteger(0);

    private volatile boolean isRunning = true;

    public BoundedSourceFunction(Path path, int checkpoints) {
      this.path = path;
      this.checkpoints = checkpoints;
    }

    @Override
    public void run(SourceContext<String> context) throws Exception {
      if (this.dataBuffer == null) {
        loadDataBuffer();
      }
      int oldCP = this.currentCP.get();
      boolean finish = false;
      while (isRunning) {
        int batchSize = this.dataBuffer.size() / this.checkpoints;
        int start = batchSize * oldCP;
        synchronized (context.getCheckpointLock()) {
          for (int i = start; i < start + batchSize; i++) {
            if (i >= this.dataBuffer.size()) {
              finish = true;
              break;
              // wait for the next checkpoint and exit
            }
            context.collect(this.dataBuffer.get(i));
          }
        }
        oldCP++;
        while (this.currentCP.get() < oldCP) {
          synchronized (context.getCheckpointLock()) {
            context.getCheckpointLock().wait(10);
          }
        }
        if (finish || !isRunning) {
          return;
        }
      }
    }

    @Override
    public void cancel() {
      this.isRunning = false;
    }

    private void loadDataBuffer() {
      try {
        this.dataBuffer = Files.readAllLines(Paths.get(this.path.toUri()));
      } catch (IOException e) {
        throw new RuntimeException("Read file " + this.path + " error", e);
      }
    }

    @Override
    public void notifyCheckpointComplete(long l) {
      this.currentCP.incrementAndGet();
    }
  }
}
