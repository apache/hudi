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

package org.apache.hudi.testsuite.writer;

import org.apache.hudi.AvroConversionUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * Spark based avro delta input writer. We don't use this yet since we cannot control result file size.
 */
public class SparkAvroDeltaInputWriter implements DeltaInputWriter<JavaRDD<GenericRecord>> {

  private static final String AVRO_FORMAT_PACKAGE = "avro";
  public SparkSession sparkSession;
  private String schemaStr;
  // TODO : the base path has to be a new path every time for spark avro
  private String basePath;

  public SparkAvroDeltaInputWriter(SparkSession sparkSession, String schemaStr, String basePath) {
    this.sparkSession = sparkSession;
    this.schemaStr = schemaStr;
    this.basePath = basePath;
  }

  @Override
  public void writeData(JavaRDD<GenericRecord> iData) throws IOException {
    AvroConversionUtils.createDataFrame(iData.rdd(), schemaStr, sparkSession).write()
        .format(AVRO_FORMAT_PACKAGE).save(basePath);
  }

  @Override
  public boolean canWrite() {
    throw new UnsupportedOperationException("not applicable for spark based writer");
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public WriteStats getWriteStats() {
    throw new UnsupportedOperationException("not applicable for spark based writer");
  }

}
