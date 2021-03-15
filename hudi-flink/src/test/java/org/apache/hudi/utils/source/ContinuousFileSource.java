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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A continuous file source that can trigger checkpoints continuously.
 *
 * <p>It loads the data in the specified file and split the data into number of checkpoints batches.
 * Say, if you want 4 checkpoints and there are 8 records in the file, the emit strategy is:
 *
 * <pre>
 *   | 2 records | 2 records | 2 records | 2 records |
 *   | cp1       | cp2       |cp3        | cp4       |
 * </pre>
 *
 * <p>If all the data are flushed out, it waits for the next checkpoint to finish and tear down the source.
 */
public class ContinuousFileSource implements StreamTableSource<RowData> {

  private final TableSchema tableSchema;
  private final Path path;
  private final Configuration conf;

  public ContinuousFileSource(
      TableSchema tableSchema,
      Path path,
      Configuration conf) {
    this.tableSchema = tableSchema;
    this.path = path;
    this.conf = conf;
  }

  @Override
  public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
    final RowType rowType = (RowType) this.tableSchema.toRowDataType().getLogicalType();
    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        new RowDataTypeInfo(rowType),
        false,
        true,
        TimestampFormat.ISO_8601);

    return execEnv.addSource(new BoundedSourceFunction(this.path, 2))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)),
            new RowDataTypeInfo(rowType));
  }

  @Override
  public TableSchema getTableSchema() {
    return this.tableSchema;
  }

  @Override
  public DataType getProducedDataType() {
    return this.tableSchema.toRowDataType().bridgedTo(RowData.class);
  }

  /**
   * Source function that partition the data into given number checkpoints batches.
   */
  public static class BoundedSourceFunction implements SourceFunction<String>, CheckpointListener {
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
      this.dataBuffer = new ArrayList<>();
      try (BufferedReader reader =
               new BufferedReader(new FileReader(this.path.toString()))) {
        String line = reader.readLine();
        while (line != null) {
          this.dataBuffer.add(line);
          // read next line
          line = reader.readLine();
        }
      } catch (IOException e) {
        throw new RuntimeException("Read file " + this.path + " error", e);
      }
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
      this.currentCP.incrementAndGet();
    }
  }
}
