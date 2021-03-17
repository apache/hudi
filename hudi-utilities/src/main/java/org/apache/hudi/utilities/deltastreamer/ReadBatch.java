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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaRDD;

/**
 * Class to hold all constituents of a batch of read in deltastreamer.
 */
public class ReadBatch {

  private SchemaProvider schemaProvider;
  private String checkpointStr;
  private JavaRDD<HoodieRecord> hoodieRecordJavaRDD;
  private String schemaStr;

  public ReadBatch(SchemaProvider schemaProvider, String checkpointStr, JavaRDD<HoodieRecord> hoodieRecordJavaRDD, String schemaStr) {
    this.schemaProvider = schemaProvider;
    this.checkpointStr = checkpointStr;
    this.hoodieRecordJavaRDD = hoodieRecordJavaRDD;
    this.schemaStr = schemaStr;
  }

  /**
   * @return the {@link SchemaProvider} as part of the batch read.
   */
  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  /**
   * @return the checkpoint string as part of the batch read.
   */
  public String getCheckpointStr() {
    return checkpointStr;
  }

  /**
   * @return the {@link JavaRDD} of {@link HoodieRecord}s as part of the batch read.
   */
  public JavaRDD<HoodieRecord> getHoodieRecordJavaRDD() {
    return hoodieRecordJavaRDD;
  }

  /**
   * @return the schema representation in string as part of the batch read.
   */
  public String getSchemaStr() {
    return schemaStr;
  }
}
