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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Class to provide schema for reading data and also writing into a Hoodie table,
 * used by deltastreamer (runs over Spark).
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class SchemaProvider implements Serializable {

  protected TypedProperties config;

  protected JavaSparkContext jssc;

  public SchemaProvider(TypedProperties props) {
    this(props, null);
  }

  protected SchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    this.config = props;
    this.jssc = jssc;
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public HoodieSchema getSourceHoodieSchema() {
    Schema schema = getSourceSchema();
    return schema == null ? null : HoodieSchema.fromAvroSchema(schema);
  }

  /**
   * Fetches the source schema from the provider.
   * @return Source schema as an Avro Schema object.
   * @deprecated since 1.2.0, use {@link #getSourceHoodieSchema()} instead.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  @Deprecated
  public Schema getSourceSchema() {
    throw new UnsupportedOperationException("getSourceSchema() is deprecated and is not implemented for this SchemaProvider. Use getSourceHoodieSchema() instead.");
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public HoodieSchema getTargetHoodieSchema() {
    Schema schema = getTargetSchema();
    return schema == null ? getSourceHoodieSchema() : HoodieSchema.fromAvroSchema(schema);
  }

  /**
   * Fetches the target schema from the provider, defaults to the source schema.
   * @return Target schema as an Avro Schema object.
   * @deprecated since 1.2.0, use {@link #getTargetHoodieSchema()} ()} instead.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  @Deprecated
  public Schema getTargetSchema() {
    // by default, use source schema as target for hoodie table as well
    return getSourceSchema();
  }

  //every schema provider has the ability to refresh itself, which will mean something different per provider.
  public void refresh() {

  }
}
