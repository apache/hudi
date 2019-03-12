/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.bench;

import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.bench.writer.FileDeltaInputWriter;
import com.uber.hoodie.bench.writer.WriteStats;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.generic.GenericRecord;

/**
 * {@link org.apache.hadoop.hdfs.DistributedFileSystem} (or {@org.apache.hadoop.hdfs.LocalFileSystem}) based delta
 * generator.
 */
public class DFSDeltaWriterAdapter implements DeltaWriterAdapter<GenericRecord> {

  private FileDeltaInputWriter deltaInputGenerator;
  private List<WriteStats> metrics = new ArrayList<>();

  public DFSDeltaWriterAdapter(FileDeltaInputWriter<GenericRecord> deltaInputGenerator) {
    this.deltaInputGenerator = deltaInputGenerator;
  }

  @Override
  public List<WriteStats> write(Iterator<GenericRecord> input) throws IOException {
    while (input.hasNext()) {
      if (this.deltaInputGenerator.canWrite()) {
        this.deltaInputGenerator.writeData(input.next());
      } else if (input.hasNext()) {
        rollOver();
      }
    }
    close();
    return this.metrics;
  }

  @VisibleForTesting
  public void rollOver() throws IOException {
    close();
    this.deltaInputGenerator = this.deltaInputGenerator.getNewWriter();
  }

  private void close() throws IOException {
    this.deltaInputGenerator.close();
    this.metrics.add(this.deltaInputGenerator.getWriteStats());
  }
}
