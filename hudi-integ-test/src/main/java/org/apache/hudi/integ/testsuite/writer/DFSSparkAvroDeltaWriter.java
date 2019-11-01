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

package org.apache.hudi.integ.testsuite.writer;

import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.integ.testsuite.writer.DeltaWriterAdapter.SparkBasedDeltaWriter;
import org.apache.spark.api.java.JavaRDD;

/**
 * NEED TO IMPLEMENT A CUSTOM SPARK PARTITIONER TO ENSURE WE WRITE LARGE ENOUGH AVRO FILES.
 */
public class DFSSparkAvroDeltaWriter implements SparkBasedDeltaWriter<JavaRDD<GenericRecord>> {

  private DeltaInputWriter<JavaRDD<GenericRecord>> deltaInputWriter;

  public DFSSparkAvroDeltaWriter(DeltaInputWriter<JavaRDD<GenericRecord>> deltaInputWriter) {
    this.deltaInputWriter = deltaInputWriter;
  }

  @Override
  public JavaRDD<DeltaWriteStats> write(JavaRDD<GenericRecord> input) throws IOException {
    this.deltaInputWriter.writeData(input);
    return null;
  }
}
