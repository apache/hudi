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

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.util.Optional;

public class InputBatch<T> {

  private final Optional<T> batch;
  private final String checkpointForNextBatch;
  private final SchemaProvider schemaProvider;

  public InputBatch(Optional<T> batch, String checkpointForNextBatch,
      SchemaProvider schemaProvider) {
    this.batch = batch;
    this.checkpointForNextBatch = checkpointForNextBatch;
    this.schemaProvider = schemaProvider;
  }

  public InputBatch(Optional<T> batch, String checkpointForNextBatch) {
    this.batch = batch;
    this.checkpointForNextBatch = checkpointForNextBatch;
    this.schemaProvider = null;
  }

  public Optional<T> getBatch() {
    return batch;
  }

  public String getCheckpointForNextBatch() {
    return checkpointForNextBatch;
  }

  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }
}
