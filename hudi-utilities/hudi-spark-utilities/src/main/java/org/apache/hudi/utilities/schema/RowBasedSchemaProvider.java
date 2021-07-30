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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.StructType;

public class RowBasedSchemaProvider extends SchemaProvider {

  // Used in GenericRecord conversions
  public static final String HOODIE_RECORD_NAMESPACE = "hoodie.source";
  public static final String HOODIE_RECORD_STRUCT_NAME = "hoodie_source";

  private StructType rowStruct;

  public RowBasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  public RowBasedSchemaProvider(StructType rowStruct) {
    super(null, null);
    this.rowStruct = rowStruct;
  }

  @Override
  public Schema getSourceSchema() {
    return AvroConversionUtils.convertStructTypeToAvroSchema(rowStruct, HOODIE_RECORD_STRUCT_NAME,
        HOODIE_RECORD_NAMESPACE);
  }
}
