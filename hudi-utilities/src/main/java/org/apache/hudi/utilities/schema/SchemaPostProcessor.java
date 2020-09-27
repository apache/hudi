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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Used in {@link SchemaProvider} to modify schema before it is passed to the caller. Can be used to
 * add marker fields in records with no fields, make everything optional, ...
 */
public abstract class SchemaPostProcessor implements Serializable {

  /** Configs supported. */
  public static class Config {
    public static final String SCHEMA_POST_PROCESSOR_PROP =
        "hoodie.deltastreamer.schemaprovider.schema_post_processor";
  }

  private static final long serialVersionUID = 1L;

  protected TypedProperties config;

  protected JavaSparkContext jssc;

  protected SchemaPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    this.config = props;
    this.jssc = jssc;
  }

  /**
   * Rewrites schema.
   *
   * @param schema input schema.
   * @return modified schema.
   */
  public abstract Schema processSchema(Schema schema);
}
