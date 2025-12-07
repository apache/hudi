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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaProvider;

import lombok.Getter;

/**
 * The default implementation for the StreamContext interface,
 * composes SchemaProvider and SourceProfileSupplier currently,
 * can be extended for other arguments in the future.
 */
@Getter
public class DefaultStreamContext implements StreamContext {

  private final SchemaProvider schemaProvider;
  private final Option<SourceProfileSupplier> sourceProfileSupplier;

  public DefaultStreamContext(SchemaProvider schemaProvider, Option<SourceProfileSupplier> sourceProfileSupplier) {
    this.schemaProvider = schemaProvider;
    this.sourceProfileSupplier = sourceProfileSupplier;
  }
}
