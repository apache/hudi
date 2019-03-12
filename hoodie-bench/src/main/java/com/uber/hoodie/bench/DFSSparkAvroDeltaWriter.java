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

import com.uber.hoodie.bench.DeltaWriterAdapter.SparkBasedDeltaWriter;
import com.uber.hoodie.bench.writer.DeltaInputWriter;
import com.uber.hoodie.bench.writer.WriteStats;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;

/**
 * NEED TO IMPLEMENT A CUSTOM SPARK PARTITIONER TO ENSURE WE WRITE LARGE ENOUGH AVRO FILES
 */
public class DFSSparkAvroDeltaWriter implements SparkBasedDeltaWriter<JavaRDD<GenericRecord>> {

  private DeltaInputWriter<JavaRDD<GenericRecord>> deltaInputWriter;

  public DFSSparkAvroDeltaWriter(DeltaInputWriter<JavaRDD<GenericRecord>> deltaInputWriter) {
    this.deltaInputWriter = deltaInputWriter;
  }

  @Override
  public JavaRDD<WriteStats> write(JavaRDD<GenericRecord> input) throws IOException {
    this.deltaInputWriter.writeData(input);
    return null;
  }
}
