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

package org.apache.hudi.client.bootstrap;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.avro.Schema;

import java.util.List;

/**
 * Bootstrap Schema Provider. Schema provided in config is used. If not available, use schema from Parquet
 */
public abstract class HoodieBootstrapSchemaProvider {

  protected final HoodieWriteConfig writeConfig;

  public HoodieBootstrapSchemaProvider(HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
  }

  /**
   * Main API to select avro schema for bootstrapping.
   * @param context HoodieEngineContext
   * @param partitions  List of partitions with files within them
   * @return Avro Schema
   */
  public final Schema getBootstrapSchema(HoodieEngineContext context, List<Pair<String, List<HoodieFileStatus>>> partitions) {
    if (writeConfig.getSchema() != null) {
      // Use schema specified by user if set
      Schema userSchema = new Schema.Parser().parse(writeConfig.getSchema());
      if (!HoodieAvroUtils.getNullSchema().equals(userSchema)) {
        return userSchema;
      }
    }
    return getBootstrapSourceSchema(context, partitions);
  }

  /**
   * Select a random file to be used to generate avro schema.
   * Override this method to get custom schema selection.
   * @param context HoodieEngineContext
   * @param partitions  List of partitions with files within them
   * @return Avro Schema
   */
  protected abstract Schema getBootstrapSourceSchema(HoodieEngineContext context,
      List<Pair<String, List<HoodieFileStatus>>> partitions);

}
