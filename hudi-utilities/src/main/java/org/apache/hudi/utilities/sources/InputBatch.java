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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaSparkContext;

public class InputBatch<T> {

  public static final HoodieSchema NULL_SCHEMA = HoodieSchema.create(HoodieSchemaType.NULL);
  private final Option<T> batch;
  private final Checkpoint checkpointForNextBatch;
  private final SchemaProvider schemaProvider;

  public InputBatch(Option<T> batch, String checkpointForNextBatch, SchemaProvider schemaProvider) {
    this(batch, new StreamerCheckpointV2(checkpointForNextBatch), schemaProvider);
  }

  public InputBatch(Option<T> batch, Checkpoint checkpointForNextBatch, SchemaProvider schemaProvider) {
    this.batch = batch;
    this.checkpointForNextBatch = checkpointForNextBatch;
    this.schemaProvider = schemaProvider;
  }

  public InputBatch(Option<T> batch, String checkpointForNextBatch) {
    this(batch, checkpointForNextBatch, null);
  }

  public InputBatch(Option<T> batch, Checkpoint checkpointForNextBatch) {
    this(batch, checkpointForNextBatch, null);
  }

  public Option<T> getBatch() {
    return batch;
  }

  public Checkpoint getCheckpointForNextBatch() {
    return checkpointForNextBatch;
  }

  public SchemaProvider getSchemaProvider() {
    if (batch.isPresent() && schemaProvider == null) {
      throw new HoodieException(
          "Schema provider is required for this operation and for the source of interest. "
              + "Please set '--schemaprovider-class' in the top level HoodieStreamer config for the source of interest. "
              + "Based on the schema provider class chosen, additional configs might be required. "
              + "For eg, if you choose 'org.apache.hudi.utilities.schema.SchemaRegistryProvider', "
              + "you may need to set configs like 'hoodie.streamer.schemaprovider.registry.url'.");
    }
    return Option.ofNullable(schemaProvider).orElseGet(NullSchemaProvider::getInstance);
  }

  public static class NullSchemaProvider extends SchemaProvider {
    private static final NullSchemaProvider INSTANCE = new NullSchemaProvider();
    public static NullSchemaProvider getInstance() {
      return INSTANCE;
    }

    private NullSchemaProvider() {
      this(null, null);
    }

    public NullSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getSourceSchema() {
      return NULL_SCHEMA;
    }
  }
}
